from dagster import asset, AssetIn, Output
from pyspark.sql import functions as F, DataFrame

COMPUTE_KIND = "SparkSQL"
LAYER = "silver"


def log_shape(context, df: DataFrame, name: str):
    context.log.info(f"{name} rows={df.count()} cols={len(df.columns)}")


@asset(
    ins={"bronze_customer": AssetIn(key_prefix=["bronze", "customer"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "customer"],
    group_name=LAYER,
)
def silver_customer(context, bronze_customer: DataFrame):
    df = bronze_customer.dropDuplicates(["customer_id"]).filter(F.col("customer_id").isNotNull())
    log_shape(context, df, "silver_customer")
    return Output(df)


@asset(
    ins={"bronze_seller": AssetIn(key_prefix=["bronze", "seller"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "seller"],
    group_name=LAYER,
)
def silver_seller(context, bronze_seller: DataFrame):
    df = bronze_seller.dropDuplicates(["seller_id"]).na.fill("")
    log_shape(context, df, "silver_seller")
    return Output(df)


@asset(
    ins={"bronze_product": AssetIn(key_prefix=["bronze", "product"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "product"],
    group_name=LAYER,
)
def silver_product(context, bronze_product: DataFrame):
    cols_to_cast = ["product_description_length", "product_length_cm", "product_height_cm", "product_width_cm"]
    df = bronze_product.dropDuplicates().na.drop()
    for col in cols_to_cast:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast("int"))
    log_shape(context, df, "silver_product")
    return Output(df)


@asset(
    ins={"bronze_order": AssetIn(key_prefix=["bronze", "order"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "order"],
    group_name=LAYER,
)
def silver_order(context, bronze_order: DataFrame):
    df = bronze_order.dropDuplicates(["order_id"]).na.drop()
    if "order_purchase_timestamp" in df.columns:
        df = df.withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
    log_shape(context, df, "silver_order")
    return Output(df)


@asset(
    ins={"bronze_order_item": AssetIn(key_prefix=["bronze", "orderitem"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "orderitem"],
    group_name=LAYER,
)
def silver_order_item(context, bronze_order_item: DataFrame):
    df = bronze_order_item.dropDuplicates().na.drop()
    if "price" in df.columns:
        df = df.withColumn("price", F.round(F.col("price"), 2).cast("double"))
    if "freight_value" in df.columns:
        df = df.withColumn("freight_value", F.round(F.col("freight_value"), 2).cast("double"))
    log_shape(context, df, "silver_order_item")
    return Output(df)


@asset(
    ins={"bronze_payment": AssetIn(key_prefix=["bronze", "payment"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "payment"],
    group_name=LAYER,
)
def silver_payment(context, bronze_payment: DataFrame):
    df = bronze_payment.dropDuplicates().na.drop()
    if "payment_value" in df.columns:
        df = df.withColumn("payment_value", F.round(F.col("payment_value"), 2).cast("double"))
    if "payment_installments" in df.columns:
        df = df.withColumn("payment_installments", F.col("payment_installments").cast("int"))
    log_shape(context, df, "silver_payment")
    return Output(df)


@asset(
    ins={"bronze_order_review": AssetIn(key_prefix=["bronze", "orderreview"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "orderreview"],
    group_name=LAYER,
)
def silver_order_review(context, bronze_order_review: DataFrame):
    df = bronze_order_review.dropDuplicates().na.drop()
    if "review_comment_title" in df.columns:
        df = df.drop("review_comment_title")
    log_shape(context, df, "silver_order_review")
    return Output(df)


@asset(
    ins={"bronze_product_category": AssetIn(key_prefix=["bronze", "productcategory"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "productcategory"],
    group_name=LAYER,
)
def silver_product_category(context, bronze_product_category: DataFrame):
    df = bronze_product_category.dropDuplicates().na.drop()
    log_shape(context, df, "silver_product_category")
    return Output(df)


@asset(
    ins={"silver_order": AssetIn(key_prefix=["silver", "order"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "date"],
    group_name=LAYER,
)
def silver_date(context, silver_order: DataFrame):
    date_df = (
        silver_order
        .select(F.col("order_purchase_timestamp").alias("ts"))
        .withColumn("full_date", F.to_date("ts"))
        .withColumn("year", F.year("ts"))
        .withColumn("month", F.month("ts"))
        .withColumn("day", F.dayofmonth("ts"))
        .withColumn("weekday", F.date_format("ts", "EEEE"))
        .dropDuplicates(["full_date"])
    )
    log_shape(context, date_df, "silver_date")
    return Output(date_df)


@asset(
    ins={"bronze_geolocation": AssetIn(key_prefix=["bronze", "geolocation"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "geolocation"],
    group_name=LAYER,
)
def silver_geolocation(context, bronze_geolocation: DataFrame):
    df = bronze_geolocation.dropDuplicates().na.drop()
    if {"geolocation_lat", "geolocation_lng"}.issubset(set(df.columns)):
        df = df.filter(
            (F.col("geolocation_lat") <= 5.27438888)
            & (F.col("geolocation_lng") >= -73.98283055)
            & (F.col("geolocation_lat") >= -33.75116944)
            & (F.col("geolocation_lng") <= -34.79314722)
        )
    log_shape(context, df, "silver_geolocation")
    return Output(df)