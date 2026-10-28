# Imagen del producer de sensores urbanos
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Usuario sin privilegios
RUN useradd --uid 1000 --create-home appuser
USER 1000

# -u: salida sin buffer, para que los logs aparezcan al instante en kubectl logs
CMD ["python", "-u", "producer.py"]
