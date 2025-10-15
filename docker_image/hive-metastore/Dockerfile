FROM openjdk:8u342-jre

# Cài đặt Python, pip, telnet, procps (để kiểm tra tiến trình)
RUN apt-get update \
 && apt-get install --assume-yes python3 python3-pip procps telnet curl \
 && apt-get clean

# Cài đặt PySpark và pandas (nếu bạn dùng cho testing)
RUN pip3 install pyspark~=3.3.2 pandas~=1.5.3

WORKDIR /opt

# Phiên bản Hadoop và Hive
ENV HADOOP_VERSION=3.2.0
ENV METASTORE_VERSION=3.0.0

# Biến môi trường quan trọng
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HIVE_HOME=/opt/apache-hive-metastore-${METASTORE_VERSION}-bin
ENV PATH=$PATH:$HADOOP_HOME/bin:$HIVE_HOME/bin

# --- Cài Hadoop, Hive Standalone Metastore và MySQL Connector ---
RUN curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    | tar -xz && \
    curl -L https://apache.org/dist/hive/hive-standalone-metastore-${METASTORE_VERSION}/hive-standalone-metastore-${METASTORE_VERSION}-bin.tar.gz \
    | tar -xz && \
    curl -L https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.19.tar.gz \
    | tar -xz && \
    cp mysql-connector-java-8.0.19/mysql-connector-java-8.0.19.jar ${HIVE_HOME}/lib/ && \
    rm -rf mysql-connector-java-8.0.19

# --- Bổ sung các file .jar bị thiếu để tránh lỗi PartitionExpressionProxy ---
RUN curl -L https://archive.apache.org/dist/hive/hive-3.0.0/apache-hive-3.0.0-bin.tar.gz \
    | tar -xz && \
    cp apache-hive-3.0.0-bin/lib/hive-exec-3.0.0.jar ${HIVE_HOME}/lib/ && \
    cp apache-hive-3.0.0-bin/lib/hive-common-3.0.0.jar ${HIVE_HOME}/lib/ && \
    cp apache-hive-3.0.0-bin/lib/hive-serde-3.0.0.jar ${HIVE_HOME}/lib/

# --- Copy file cấu hình và entrypoint ---
COPY metastore-site.xml ${HIVE_HOME}/conf/metastore-site.xml
COPY entrypoint.sh /entrypoint.sh

# --- Phân quyền cho user hive ---
RUN groupadd -r hive --gid=1000 && \
    useradd -r -g hive --uid=1000 -d ${HIVE_HOME} hive && \
    chown -R hive:hive ${HIVE_HOME} && \
    chown hive:hive /entrypoint.sh && chmod +x /entrypoint.sh

USER hive

EXPOSE 9083

ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]