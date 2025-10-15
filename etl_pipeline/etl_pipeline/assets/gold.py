from dagster import asset, AssetIn, Output
from pyspark.sql import functions as F, DataFrame

COMPUTE_KIND = "SparkSQL"
LAYER = "gold"


def log_shape(context, df: DataFrame, name: str):
    context.log.info(f"{name} rows={df.count()} cols={len(df.columns)}")


# --------------------
# DIMENSIONS
# --------------------

@asset(
    ins={"silver_customer": AssetIn(key_prefix=["silver", "customer"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "dimcustomer"],
    group_name=LAYER,
)
def dim_customer(context, silver_customer: DataFrame):
    df = silver_customer.dropDuplicates(["customer_id"])
    log_shape(context, df, "dim_customer")
    return Output(df)


@asset(
    ins={"silver_seller": AssetIn(key_prefix=["silver", "seller"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "dimseller"],
    group_name=LAYER,
)
def dim_seller(context, silver_seller: DataFrame):
    df = silver_seller.dropDuplicates(["seller_id"])
    log_shape(context, df, "dim_seller")
    return Output(df)


@asset(
    ins={"silver_product": AssetIn(key_prefix=["silver", "product"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "dimproduct"],
    group_name=LAYER,
)
def dim_product(context, silver_product: DataFrame):
    df = silver_product.dropDuplicates(["product_id"])
    log_shape(context, df, "dim_product")
    return Output(df)


@asset(
    ins={"silver_product_category": AssetIn(key_prefix=["silver", "productcategory"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "dimproductcategory"],
    group_name=LAYER,
)
def dim_product_category(context, silver_product_category: DataFrame):
    df = silver_product_category.dropDuplicates(["product_category_name"])
    log_shape(context, df, "dim_product_category")
    return Output(df)


@asset(
    ins={"silver_date": AssetIn(key_prefix=["silver", "date"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "dimdate"],
    group_name=LAYER,
)
def dim_date(context, silver_date: DataFrame):
    df = silver_date.dropDuplicates(["full_date"])
    log_shape(context, df, "dim_date")
    return Output(df)


@asset(
    ins={"silver_geolocation": AssetIn(key_prefix=["silver", "geolocation"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "dimgeolocation"],
    group_name=LAYER,
)
def dim_geolocation(context, silver_geolocation: DataFrame):
    df = silver_geolocation.dropDuplicates(["geolocation_zip_code_prefix"])
    log_shape(context, df, "dim_geolocation")
    return Output(df)


# --------------------
# FACTS
# --------------------

@asset(
    ins={
        "silver_order": AssetIn(key_prefix=["silver", "order"]),
        "silver_order_item": AssetIn(key_prefix=["silver", "orderitem"]),
        "silver_payment": AssetIn(key_prefix=["silver", "payment"]),
        "silver_order_review": AssetIn(key_prefix=["silver", "orderreview"]),
    },
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "factorder"],
    group_name=LAYER,
)
def fact_order(context, silver_order, silver_order_item, silver_payment, silver_order_review):
    df = (
        silver_order.alias("o")
        .join(silver_order_item.alias("i"), "order_id", "left")
        .join(silver_payment.alias("p"), "order_id", "left")
        .join(silver_order_review.alias("r"), "order_id", "left")
    )
    log_shape(context, df, "fact_order")
    return Output(df)