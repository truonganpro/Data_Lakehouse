from dagster import IOManager, InputContext, OutputContext
from pyspark.sql import SparkSession, DataFrame
import os


class SparkIOManager(IOManager):
    def __init__(self, config=None):
        # Ensure endpoint URL has proper protocol
        endpoint_url = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        if not endpoint_url.startswith("http"):
            endpoint_url = f"http://{endpoint_url}"
            
        default_config = {
            "endpoint_url": endpoint_url,
            "minio_access_key": os.getenv("MINIO_ACCESS_KEY", "minio"),
            "minio_secret_key": os.getenv("MINIO_SECRET_KEY", "minio123"),
            "jdbc_jar_path": os.getenv("JDBC_JAR_PATH", "/opt/jars/mysql-connector-j-8.0.33.jar"),
            "jars_dir": os.getenv("JARS_DIR", "/opt/jars"),
            "warehouse": os.getenv("SPARK_WAREHOUSE", "s3a://lakehouse/"),
            "master": os.getenv("SPARK_MASTER", "spark://spark-master:7077"),
        }
        self._config = {**default_config, **(config or {})}
        self._spark = None

    def _get_spark(self) -> SparkSession:
        if self._spark is None:
            jdbc_jar = self._config["jdbc_jar_path"]
            jars_dir = self._config["jars_dir"]
            
            # Validate required JARs exist
            required_jars = [
                ("JDBC jar", jdbc_jar),
                ("Delta Core", os.path.join(jars_dir, "delta-core_2.12-2.3.0.jar")),
                ("Delta Storage", os.path.join(jars_dir, "delta-storage-2.3.0.jar")),
                ("Hadoop AWS", os.path.join(jars_dir, "hadoop-aws-3.3.2.jar")),
                ("AWS SDK Bundle", os.path.join(jars_dir, "aws-java-sdk-bundle-1.11.1026.jar")),
            ]
            
            for jar_name, jar_path in required_jars:
                if not os.path.exists(jar_path):
                    raise FileNotFoundError(f"Missing {jar_name}: {jar_path}")

            spark_jars = ",".join([
                f"file://{jdbc_jar}",
                f"file://{jars_dir}/delta-core_2.12-2.3.0.jar",
                f"file://{jars_dir}/delta-storage-2.3.0.jar",
                f"file://{jars_dir}/hadoop-aws-3.3.2.jar",
                f"file://{jars_dir}/aws-java-sdk-bundle-1.11.1026.jar",
            ])

            builder = (
                SparkSession.builder
                .master(self._config["master"])
                .appName("Dagster SparkIOManager")
                .config("spark.jars", spark_jars)
                .config("spark.driver.extraClassPath", jdbc_jar)
                .config("spark.executor.extraClassPath", jdbc_jar)
                .config("spark.driver.userClassPathFirst", "true")
                .config("spark.executor.userClassPathFirst", "true")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.hadoop.fs.s3a.endpoint", self._config["endpoint_url"])
                .config("spark.hadoop.fs.s3a.access.key", self._config["minio_access_key"])
                .config("spark.hadoop.fs.s3a.secret.key", self._config["minio_secret_key"])
                .config("spark.hadoop.fs.s3a.path.style-access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .config("spark.sql.warehouse.dir", self._config["warehouse"])
                # Hive Metastore configuration
                .config("hive.metastore.uris", "thrift://hive-metastore:9083")
                .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
                .config("spark.sql.catalogImplementation", "hive")
                .enableHiveSupport()
            )

            self._spark = builder.getOrCreate()
        return self._spark

    def handle_output(self, context: OutputContext, obj: DataFrame):
        full_path = context.asset_key.path
        if len(full_path) < 2:
            raise ValueError("Unexpected asset_key.path: " + str(full_path))

        layer = full_path[0]       # ví dụ: "bronze"
        table = full_path[-1]      # ví dụ: "customer"
        
        # Use s3a:// for data writing (Spark/Hadoop compatible)
        path_s3a = f"{self._config['warehouse']}{layer}/{table}"      # s3a://lakehouse/layer/table
        # Use s3:// for metadata registration (Trino compatible)
        path_s3 = path_s3a.replace("s3a://", "s3://", 1)             # s3://lakehouse/layer/table
        db_loc_s3 = f"{self._config['warehouse'].replace('s3a://','s3://',1)}{layer}"

        spark = self._get_spark()
        context.log.info(f"(SparkIOManager) Saving table: {layer}.{table} -> {path_s3a}")

        # 1) Ghi file Delta vật lý ra MinIO (s3a)
        obj.write.format("delta").mode("overwrite").save(path_s3a)

        # 2) Đăng ký metadata vào Hive Metastore với LOCATION dùng 's3://'
        try:
            # Create database with s3:// location
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {layer} LOCATION '{db_loc_s3}'")
            
            # Drop existing table if any (to avoid s3a:// vs s3:// conflicts)
            spark.sql(f"DROP TABLE IF EXISTS {layer}.{table}")
            
            # Create table with s3:// location for Trino compatibility
            spark.sql(f"CREATE TABLE {layer}.{table} USING DELTA LOCATION '{path_s3}'")
            context.log.info(f"Successfully registered {layer}.{table} in Hive Metastore with s3:// location")
            
        except Exception as e:
            context.log.warning(f"Failed to register in Hive Metastore: {e}")
            # Fallback
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {layer}")
            obj.write.format("delta").mode("overwrite").option("path", path_s3a).saveAsTable(f"{layer}.{table}")

        context.add_output_metadata({
            "path_data": path_s3a,
            "path_registered": path_s3,
            "rows": obj.count(),
            "columns": obj.columns,
        })

    def load_input(self, context: InputContext) -> DataFrame:
        full_path = context.asset_key.path
        if len(full_path) < 2:
            raise ValueError("Unexpected asset_key.path: " + str(full_path))

        layer = full_path[0]
        table = full_path[-1]
        path = f"{self._config['warehouse']}{layer}/{table}"

        spark = self._get_spark()
        context.log.info(f"(SparkIOManager) Loading table: {layer}.{table} from {path}")

        try:
            df = spark.read.format("delta").load(path)
        except Exception as e:
            context.log.warning(f"(SparkIOManager) Failed to load {path}, error: {e}")
            raise

        return df