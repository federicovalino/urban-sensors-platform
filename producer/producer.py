"""
Producer de sensores urbanos.

Simula una red de sensores de una ciudad y publica una lectura JSON por
intervalo en el tópico de Kafka (por defecto urban_sensors):

    {
      "sensor_id": "sensor-03",
      "temperature": 21.4,
      "humidity": 63.0,
      "air_quality_index": 48,
      "timestamp": "2026-10-28T21:05:12+00:00"
    }

- Key del mensaje = sensor_id: las lecturas de un sensor van siempre a la misma
  partición y llegan en orden.
- Un porcentaje configurable de lecturas trae un campo nulo, para ejercitar el
  filtrado del job de Spark.
- Toda la configuración llega por variables de entorno (ConfigMaps en K8s):
  no hay direcciones ni credenciales en el código.
"""

import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import KafkaException, Producer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "urban_sensors")
INTERVAL_SECONDS = float(os.environ.get("PRODUCER_INTERVAL_SECONDS", "1"))
SENSOR_COUNT = int(os.environ.get("SENSOR_COUNT", "6"))
NULL_RATE = float(os.environ.get("NULL_RATE", "0.05"))
HEARTBEAT_FILE = Path(os.environ.get("HEARTBEAT_FILE", "/tmp/heartbeat"))

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-7s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("sensor-producer")

NULLABLE_FIELDS = ["temperature", "humidity", "air_quality_index"]

# Cada sensor tiene un "clima base" distinto, así los promedios por sensor se diferencian.
SENSORS = {
    f"sensor-{i:02d}": {
        "temp_base": random.uniform(14, 26),
        "hum_base": random.uniform(45, 75),
        "aqi_base": random.uniform(25, 90),
    }
    for i in range(1, SENSOR_COUNT + 1)
}

running = True


def stop(signum, _frame) -> None:
    """Kubernetes envía SIGTERM al borrar o actualizar el Pod: se corta el loop y se hace flush."""
    global running
    log.info("Señal %s recibida, deteniendo producer...", signum)
    running = False


def build_reading(sensor_id: str) -> dict:
    """Lectura sintética alrededor del clima base del sensor."""
    base = SENSORS[sensor_id]
    reading = {
        "sensor_id": sensor_id,
        "temperature": round(random.gauss(base["temp_base"], 1.5), 1),
        "humidity": round(min(100.0, max(0.0, random.gauss(base["hum_base"], 5))), 1),
        # AQI con picos ocasionales de contaminación (+80 en ~20 % de las lecturas)
        "air_quality_index": int(max(0, random.gauss(base["aqi_base"], 12)) + random.choice([0, 0, 0, 0, 80])),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    if random.random() < NULL_RATE:
        reading[random.choice(NULLABLE_FIELDS)] = None
    return reading


def delivery_report(err, msg) -> None:
    if err is not None:
        log.error("Entrega fallida: %s", err)
        return
    log.info("ENVIADO p%s off %-6s %s", msg.partition(), msg.offset(), msg.value().decode("utf-8"))


def wait_for_topic(producer: Producer) -> int:
    """Espera a que el tópico exista (lo crea el Job kafka-topic-init)."""
    while running:
        HEARTBEAT_FILE.touch()  # esperar al tópico no es estar colgado
        try:
            topic = producer.list_topics(topic=TOPIC, timeout=10).topics.get(TOPIC)
            if topic is not None and topic.error is None:
                return len(topic.partitions)
            log.warning("El tópico '%s' todavía no existe, reintentando en 5 s...", TOPIC)
        except KafkaException as exc:
            log.warning("Kafka no disponible en %s (%s), reintentando en 5 s...", BOOTSTRAP_SERVERS, exc)
        time.sleep(5)
    return 0


def main() -> None:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "sensor-producer",
        "acks": "all",
    })

    partitions = wait_for_topic(producer)
    if not running:
        return
    log.info("Conectado a %s | tópico '%s' con %d particiones | %d sensores | 1 lectura cada %ss",
             BOOTSTRAP_SERVERS, TOPIC, partitions, len(SENSORS), INTERVAL_SECONDS)

    sent = 0
    while running:
        sensor_id = random.choice(list(SENSORS))
        reading = build_reading(sensor_id)
        producer.produce(TOPIC, key=sensor_id, value=json.dumps(reading).encode("utf-8"),
                         on_delivery=delivery_report)
        producer.poll(0)
        sent += 1
        HEARTBEAT_FILE.touch()  # la livenessProbe verifica que el loop siga vivo
        time.sleep(INTERVAL_SECONDS)

    pending = producer.flush(10)
    log.info("Producer detenido | enviados: %d | pendientes sin confirmar: %d", sent, pending)


if __name__ == "__main__":
    main()
