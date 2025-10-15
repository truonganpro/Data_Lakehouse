#!/usr/bin/env python3
"""
Test script để kiểm tra kết nối Spark ↔ MinIO ↔ Hive Metastore
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

def create_spark_session():
    """Tạo Spark session với cấu hình MinIO và Hive Metastore"""
    
    spark = SparkSession.builder \
        .appName("TestMinIOConnection") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.warehouse.dir", "s3a://lakehouse/") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("hive.metastore.warehouse.dir", "s3a://lakehouse/") \
        .enableHiveSupport() \
        .getOrCreate()
    
    return spark

def test_minio_connection(spark):
    """Test kết nối MinIO"""
    print("🔍 Testing MinIO connection...")
    
    try:
        # Test đọc bucket lakehouse
        df = spark.read.format("delta").load("s3a://lakehouse/")
        print("✅ Successfully connected to MinIO lakehouse bucket")
        return True
    except Exception as e:
        print(f"❌ Failed to connect to MinIO: {e}")
        return False

def test_hive_metastore(spark):
    """Test kết nối Hive Metastore"""
    print("🔍 Testing Hive Metastore connection...")
    
    try:
        # Test tạo database
        spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
        print("✅ Successfully connected to Hive Metastore")
        return True
    except Exception as e:
        print(f"❌ Failed to connect to Hive Metastore: {e}")
        return False

def create_test_delta_table(spark):
    """Tạo Delta table test trong các layer"""
    print("🔍 Creating test Delta tables...")
    
    try:
        # Tạo sample data
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", IntegerType(), True)
        ])
        
        data = [(1, "test1", 100), (2, "test2", 200), (3, "test3", 300)]
        df = spark.createDataFrame(data, schema)
        
        # Tạo các layer
        layers = ["bronze", "silver", "gold", "platium"]
        
        for layer in layers:
            table_path = f"s3a://lakehouse/{layer}/test_table"
            df.write.format("delta").mode("overwrite").save(table_path)
            print(f"✅ Created test table in {layer} layer: {table_path}")
            
            # Đăng ký table với Hive Metastore
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS test_db.test_table_{layer}
                USING DELTA
                LOCATION '{table_path}'
            """)
            print(f"✅ Registered table in Hive Metastore: test_db.test_table_{layer}")
        
        return True
    except Exception as e:
        print(f"❌ Failed to create test Delta tables: {e}")
        return False

def main():
    print("🚀 Starting Spark MinIO Hive Metastore Test")
    print("=" * 50)
    
    # Tạo Spark session
    spark = create_spark_session()
    
    # Test kết nối MinIO
    minio_ok = test_minio_connection(spark)
    
    # Test kết nối Hive Metastore
    hive_ok = test_hive_metastore(spark)
    
    # Tạo test tables nếu cả hai kết nối đều OK
    if minio_ok and hive_ok:
        create_test_delta_table(spark)
    
    print("=" * 50)
    print("🏁 Test completed!")
    
    spark.stop()

if __name__ == "__main__":
    main()
