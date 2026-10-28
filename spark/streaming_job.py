"""
streaming_job.py — Spark Structured Streaming sobre el tópico urban_sensors.

Pipeline (DataFrames):
    Kafka (bytes) -> from_json con esquema estricto
      -> FILTRADO     descarta lecturas con nulos o valores fuera de rango
      -> PROYECCIÓN   event_time (timestamp real) y categoría de calidad del aire
      -> AGREGACIÓN   ventana tumbling de 1 minuto por sensor_id:
                      promedio de temperatura y de air_quality_index
                      (+ humedad promedio, AQI máximo y cantidad de lecturas)
      -> SINK         consola (logs del Pod), modo update

Toda la configuración llega por variables de entorno (ConfigMaps en K8s).
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "urban_sensors")
WINDOW_DURATION = os.environ.get("WINDOW_DURATION", "1 minute")
WATERMARK_DELAY = os.environ.get("WATERMARK_DELAY", "30 seconds")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "15 seconds")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "latest")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/checkpoints")

# Esquema ESTRICTO: en streaming Spark no puede inferirlo. Un campo con tipo
# incorrecto queda en null y lo descarta el filtrado.
SENSOR_SCHEMA = StructType([
    StructField("sensor_id", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("air_quality_index", DoubleType()),
    StructField("timestamp", StringType()),
])


def read_kafka(spark: SparkSession) -> DataFrame:
    """Fuente: tópico de Kafka. El value llega en bytes y se parsea con el esquema."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    return raw.select(
        F.from_json(F.col("value").cast("string"), SENSOR_SCHEMA).alias("r")
    ).select("r.*")


def filter_valid(df: DataFrame) -> DataFrame:
    """FILTRADO: sin nulos y con valores físicamente posibles."""
    return df.filter(
        F.col("sensor_id").isNotNull()
        & F.col("timestamp").isNotNull()
        & F.col("temperature").between(-30, 60)
        & F.col("humidity").between(0, 100)
        & F.col("air_quality_index").between(0, 500)
    )  # between() con un valor null da null -> la fila se descarta


def project(df: DataFrame) -> DataFrame:
    """PROYECCIÓN: timestamp real y categoría de calidad del aire (escala AQI)."""
    return (
        df.withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn(
            "aqi_category",
            F.when(F.col("air_quality_index") <= 50, "BUENA")
            .when(F.col("air_quality_index") <= 100, "MODERADA")
            .when(F.col("air_quality_index") <= 150, "DAÑINA_SENSIBLES")
            .otherwise("DAÑINA"),
        )
        .filter(F.col("event_time").isNotNull())
    )


def aggregate(df: DataFrame) -> DataFrame:
    """AGREGACIÓN: ventana de 1 minuto por sensor_id (requisito de la consigna)."""
    return (
        df.withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(F.window("event_time", WINDOW_DURATION), "sensor_id")
        .agg(
            F.round(F.avg("temperature"), 2).alias("avg_temperature"),
            F.round(F.avg("air_quality_index"), 1).alias("avg_air_quality_index"),
            F.round(F.avg("humidity"), 1).alias("avg_humidity"),
            F.max("air_quality_index").cast("int").alias("max_aqi"),
            F.count("*").alias("readings"),
        )
        .select(
            F.date_format("window.start", "HH:mm:ss").alias("window_start"),
            F.date_format("window.end", "HH:mm:ss").alias("window_end"),
            "sensor_id", "avg_temperature", "avg_air_quality_index",
            "avg_humidity", "max_aqi", "readings",
        )
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("urban-sensors-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    windows = aggregate(project(filter_valid(read_kafka(spark))))

    # .start() es la ACCIÓN: hasta acá solo se construyó el plan.
    query = (
        windows.writeStream
        .queryName("urban_sensors_1min")
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 100)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/urban_sensors_1min")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    print(f"[urban-sensors] Streaming iniciado | {BOOTSTRAP_SERVERS} | tópico {TOPIC} | "
          f"ventana {WINDOW_DURATION} | watermark {WATERMARK_DELAY} | trigger {TRIGGER_INTERVAL}", flush=True)

    # Sin esto el proceso termina y la query muere.
    query.awaitTermination()


if __name__ == "__main__":
    main()
