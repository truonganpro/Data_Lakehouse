#!/bin/bash
set -e

JARS_DIR="./jars"
mkdir -p $JARS_DIR

download_jar() {
  local url=$1
  local file=$2
  if [ -f "$JARS_DIR/$file" ]; then
    echo "⚡ $file đã tồn tại, bỏ qua..."
  else
    echo "📥 Đang tải $file ..."
    curl -fSL "$url" -o "$JARS_DIR/$file"
  fi
}

download_jar "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" "mysql-connector-j-8.0.33.jar"
download_jar "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar" "delta-core_2.12-2.3.0.jar"
download_jar "https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar" "delta-storage-2.3.0.jar"
download_jar "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar" "hadoop-aws-3.3.2.jar"
download_jar "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar" "aws-java-sdk-bundle-1.11.1026.jar"

echo "✅ Hoàn tất! Các file đã nằm trong $JARS_DIR:"
ls -lh $JARS_DIR