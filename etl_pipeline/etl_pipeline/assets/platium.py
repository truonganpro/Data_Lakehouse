from dagster import asset, AssetIn, Output
from pyspark.sql import functions as F, DataFrame

COMPUTE_KIND = "SparkSQL"
LAYER = "platium"


def log_shape(context, df: DataFrame, name: str):
    context.log.info(f"{name} rows={df.count()} cols={len(df.columns)}")


@asset(
    ins={
        "fact_order": AssetIn(key_prefix=["gold", "factorder"]),
        "dim_customer": AssetIn(key_prefix=["gold", "dimcustomer"]),
        "dim_seller": AssetIn(key_prefix=["gold", "dimseller"]),
        "dim_product": AssetIn(key_prefix=["gold", "dimproduct"]),
        "dim_product_category": AssetIn(key_prefix=["gold", "dimproductcategory"]),
        "dim_date": AssetIn(key_prefix=["gold", "dimdate"]),
        "dim_geolocation": AssetIn(key_prefix=["gold", "dimgeolocation"]),
    },
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "sale"],
    group_name=LAYER,
)
def cube_sale(
    context,
    fact_order,
    dim_customer,
    dim_seller,
    dim_product,
    dim_product_category,
    dim_date,
    dim_geolocation,
):
    # join cơ bản
    df = (
        fact_order.alias("f")
        .join(dim_customer.alias("c"), "customer_id", "left")
        .join(dim_seller.alias("s"), "seller_id", "left")
        .join(dim_product.alias("p"), "product_id", "left")
        .join(dim_product_category.alias("pc"), "product_category_name", "left")
        .join(
            dim_date.alias("d"),
            F.to_date("f.order_purchase_timestamp") == F.col("d.full_date"),
            "left",
        )
    )

    # join với geolocation cho customer
    df = df.join(
        dim_geolocation
        .withColumnRenamed("geolocation_zip_code_prefix", "customer_zip_code_prefix")
        .withColumnRenamed("geolocation_city", "customer_city_full")
        .withColumnRenamed("geolocation_state", "customer_state_full")
        .withColumnRenamed("geolocation_lat", "customer_lat")
        .withColumnRenamed("geolocation_lng", "customer_lng"),
        on="customer_zip_code_prefix",
        how="left",
    )

    # join với geolocation cho seller
    df = df.join(
        dim_geolocation
        .withColumnRenamed("geolocation_zip_code_prefix", "seller_zip_code_prefix")
        .withColumnRenamed("geolocation_city", "seller_city_full")
        .withColumnRenamed("geolocation_state", "seller_state_full")
        .withColumnRenamed("geolocation_lat", "seller_lat")
        .withColumnRenamed("geolocation_lng", "seller_lng"),
        on="seller_zip_code_prefix",
        how="left",
    )

    log_shape(context, df, "cube_sale")
    return Output(df)