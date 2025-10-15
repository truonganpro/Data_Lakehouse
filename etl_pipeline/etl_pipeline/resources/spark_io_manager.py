from dagster import IOManager, InputContext, OutputContext
from pyspark.sql import SparkSession, DataFrame
import os


class SparkIOManager(IOManager):
    def __init__(self, config=None):
        default_config = {
            "endpoint_url": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
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
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .config("spark.sql.warehouse.dir", self._config["warehouse"])
            )

            self._spark = builder.getOrCreate()
        return self._spark

    def handle_output(self, context: OutputContext, obj: DataFrame):
        full_path = context.asset_key.path
        if len(full_path) < 2:
            raise ValueError("Unexpected asset_key.path: " + str(full_path))

        layer = full_path[0]     # ví dụ: "bronze"
        table = full_path[-1]    # ví dụ: "customer"
        path = f"{self._config['warehouse']}{layer}/{table}"

        spark = self._get_spark()
        context.log.info(f"(SparkIOManager) Saving table: {layer}.{table} -> {path}")

        # Ghi delta trực tiếp ra MinIO
        obj.write.format("delta").mode("overwrite").save(path)

        # Đăng ký vào Spark session để query được ngay
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {layer} LOCATION '{self._config['warehouse']}{layer}'")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {layer}.{table} USING DELTA LOCATION '{path}'")

        context.add_output_metadata({
            "path": path,
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