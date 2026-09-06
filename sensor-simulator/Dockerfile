# Sensor simulator: a Python load generator producing to Kafka `city-telemetry`.
#   docker build -t smartcity/sensor-simulator:dev .
#   docker run --rm -e KAFKA_BOOTSTRAP=host.docker.internal:9092 smartcity/sensor-simulator:dev --extra-sensors 997 --interval 0.1
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY sensor_simulator.py ./
RUN useradd --system --uid 1000 sim
USER sim
# -u: unbuffered stdout so the delivery counters show up in `kubectl logs` as they happen.
ENTRYPOINT ["python", "-u", "sensor_simulator.py"]
