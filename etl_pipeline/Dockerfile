FROM python:3.9-slim-bullseye

ENV DEBIAN_FRONTEND=noninteractive

# Cài Java 17 (ổn định cho Spark/Delta), certificate và libs cơ bản
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        build-essential \
        curl \
        ca-certificates \
        libssl-dev \
        libffi-dev \
        libxml2-dev \
        libxslt1-dev \
        zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*

# ---- Thêm JDBC jar cho MySQL ----
RUN mkdir -p /opt/jars && \
    curl -fSL https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar \
         -o /opt/jars/mysql-connector-j-8.0.33.jar

# Set working directory
WORKDIR /opt/dagster/app/

# Copy requirements và cài dependencies
COPY requirements.txt /opt/dagster/app/etl_pipeline/requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r etl_pipeline/requirements.txt

# Copy code
COPY etl_pipeline /opt/dagster/app/etl_pipeline

# Đảm bảo module tìm thấy
ENV PYTHONPATH="/opt/dagster/app:${PYTHONPATH}"

# Env để SparkIOManager biết JDBC jar path
ENV JDBC_JAR_PATH=/opt/jars/mysql-connector-j-8.0.33.jar

# Command mặc định: chạy Dagster API gRPC
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]