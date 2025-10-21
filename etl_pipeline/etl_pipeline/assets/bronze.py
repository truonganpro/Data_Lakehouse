from dagster import asset, Output
from pyspark.sql import DataFrame

COMPUTE_KIND = "SparkSQL"
LAYER = "bronze"


def log_shape(context, df: DataFrame, name: str):
    context.log.info(f"{name} rows={df.count()} cols={len(df.columns)}")


def load_mysql_table(context, table_name: str) -> DataFrame:
    spark = context.resources.spark_io_manager._get_spark()

    jdbc_url = (
        "jdbc:mysql://de_mysql:3306/brazillian_ecommerce"
        "?zeroDateTimeBehavior=CONVERT_TO_NULL"
        "&serverTimezone=UTC"
        "&useSSL=false"
        "&allowPublicKeyRetrieval=true"
    )

    df = spark.read.format("jdbc").options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=table_name,
        user="root",
        password="admin123",
    ).load()

    log_shape(context, df, f"bronze_{table_name}")
    return df


@asset(io_manager_key="spark_io_manager", compute_kind=COMPUTE_KIND, key_prefix=[LAYER, "customer"], group_name=LAYER)
def bronze_customer(context):
    return Output(load_mysql_table(context, "customers"))   # MySQL: customers → asset/table: bronze.customer

@asset(io_manager_key="spark_io_manager", compute_kind=COMPUTE_KIND, key_prefix=[LAYER, "seller"], group_name=LAYER)
def bronze_seller(context):
    return Output(load_mysql_table(context, "sellers"))

@asset(io_manager_key="spark_io_manager", compute_kind=COMPUTE_KIND, key_prefix=[LAYER, "product"], group_name=LAYER)
def bronze_product(context):
    return Output(load_mysql_table(context, "products"))

@asset(io_manager_key="spark_io_manager", compute_kind=COMPUTE_KIND, key_prefix=[LAYER, "order"], group_name=LAYER)
def bronze_order(context):
    return Output(load_mysql_table(context, "orders"))

@asset(io_manager_key="spark_io_manager", compute_kind=COMPUTE_KIND, key_prefix=[LAYER, "orderitem"], group_name=LAYER)
def bronze_order_item(context):
    return Output(load_mysql_table(context, "order_items"))

@asset(io_manager_key="spark_io_manager", compute_kind=COMPUTE_KIND, key_prefix=[LAYER, "payment"], group_name=LAYER)
def bronze_payment(context):
    return Output(load_mysql_table(context, "order_payments"))

@asset(io_manager_key="spark_io_manager", compute_kind=COMPUTE_KIND, key_prefix=[LAYER, "orderreview"], group_name=LAYER)
def bronze_order_review(context):
    return Output(load_mysql_table(context, "order_reviews"))

@asset(io_manager_key="spark_io_manager", compute_kind=COMPUTE_KIND, key_prefix=[LAYER, "geolocation"], group_name=LAYER)
def bronze_geolocation(context):
    return Output(load_mysql_table(context, "geolocation"))

@asset(io_manager_key="spark_io_manager", compute_kind=COMPUTE_KIND, key_prefix=[LAYER, "productcategory"], group_name=LAYER)
def bronze_product_category(context):
    return Output(load_mysql_table(context, "product_category_name_translation"))