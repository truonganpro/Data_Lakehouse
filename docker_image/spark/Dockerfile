FROM docker.io/bitnami/spark:3.3.2

USER root

# Install prerequisites
RUN apt-get update && apt-get install -y curl && apt-get clean

# Copy danh sách JARs vào container
COPY requirements-jars.txt /tmp/requirements-jars.txt

# Download tất cả JARs về thư mục jars/
RUN set -ex && \
    while read url; do \
      echo "Downloading $url" && \
      curl -fSL $url -o /opt/bitnami/spark/jars/$(basename $url); \
    done < /tmp/requirements-jars.txt