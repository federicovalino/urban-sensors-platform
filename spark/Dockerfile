# ---------------------------------------------------------------------------
# Imagen del job de Spark Structured Streaming.
# Base: imagen oficial de Spark 3.5.3 con Python. El conector de Kafka y sus
# dependencias se incluyen en la imagen (en lugar de --packages) para que el
# Pod no dependa de descargar nada al arrancar.
# ---------------------------------------------------------------------------
FROM spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

ARG MAVEN=https://repo1.maven.org/maven2
ADD ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/*.jar

WORKDIR /opt/app
COPY streaming_job.py .

# Usuario sin privilegios de la imagen oficial
USER spark

ENV PYTHONUNBUFFERED=1
