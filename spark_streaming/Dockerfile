FROM bitnami/spark:3.5.0

USER root

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy streaming job
COPY . .

# Expose Spark UI
EXPOSE 4040

CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0", \
     "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
     "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
     "/app/anomaly_detector.py"]
