from dagster import asset, AssetIn, Output
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import *

COMPUTE_KIND = "SparkSQL"
LAYER = "gold"

def log_shape(context, df: DataFrame, name: str):
    context.log.info(f"{name} rows={df.count()} cols={len(df.columns)}")

# --------------------
# IMPROVED DIMENSIONS
# --------------------

@asset(
    ins={"silver_customer": AssetIn(key_prefix=["silver", "customer"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "dimcustomer"],
    group_name=LAYER,
)
def dim_customer(context, silver_customer: DataFrame):
    """Customer dimension with enhanced attributes"""
    df = (silver_customer
          .dropDuplicates(["customer_id"])
          .withColumn("customer_unique_id", F.col("customer_unique_id"))  # Keep for tracking
          .withColumn("city_state", F.concat_ws(", ", F.col("customer_city"), F.col("customer_state")))
          .select(
              "customer_id",
              "customer_unique_id", 
              "customer_zip_code_prefix",
              "customer_city",
              "customer_state",
              "city_state"
          )
    )
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
    """Seller dimension with enhanced attributes"""
    df = (silver_seller
          .dropDuplicates(["seller_id"])
          .withColumn("city_state", F.concat_ws(", ", F.col("seller_city"), F.col("seller_state")))
    )
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
    """Product dimension with enhanced attributes"""
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
    """Product category dimension with English translation"""
    df = (silver_product_category
          .dropDuplicates(["product_category_name"])
          .withColumn("product_category_name_english", F.col("product_category_name_english"))
    )
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
    """Date dimension with enhanced time attributes"""
    df = (silver_date
          .dropDuplicates(["full_date"])
          .withColumn("year_month", F.date_format(F.col("full_date"), "yyyy-MM"))
          .withColumn("quarter", F.quarter(F.col("full_date")))
          .withColumn("week_of_year", F.weekofyear(F.col("full_date")))
    )
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
    """Geolocation dimension with enhanced attributes"""
    df = (silver_geolocation
          .dropDuplicates(["geolocation_zip_code_prefix"])
          .withColumn("city_state", F.concat_ws(", ", F.col("geolocation_city"), F.col("geolocation_state")))
    )
    log_shape(context, df, "dim_geolocation")
    return Output(df)

# --------------------
# IMPROVED FACTS WITH PROPER GRAIN
# --------------------

@asset(
    ins={
        "silver_order": AssetIn(key_prefix=["silver", "order"]),
        "silver_order_item": AssetIn(key_prefix=["silver", "orderitem"]),
    },
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "factorderitem"],
    group_name=LAYER,
)
def fact_order_item(context, silver_order, silver_order_item):
    """Fact table with grain: 1 row per order_id + order_item_id"""
    df = (silver_order_item
          .join(silver_order.select("order_id", "customer_id", "order_purchase_timestamp", "order_status"), 
                "order_id", "left")
          .select(
              "order_id",
              "order_item_id", 
              "product_id",
              "seller_id",
              "customer_id",
              F.col("order_purchase_timestamp").cast("date").alias("full_date"),
              "price",
              "freight_value",
              "shipping_limit_date",
              "order_status"
          )
    )
    log_shape(context, df, "fact_order_item")
    return Output(df)

@asset(
    ins={
        "silver_order": AssetIn(key_prefix=["silver", "order"]),
        "silver_payment": AssetIn(key_prefix=["silver", "payment"]),
        "fact_order_item": AssetIn(key_prefix=[LAYER, "factorderitem"]),
    },
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "factorder"],
    group_name=LAYER,
)
def fact_order(context, silver_order, silver_payment, fact_order_item):
    """Fact table with grain: 1 row per order_id"""
    
    # Aggregate from fact_order_item
    order_item_agg = (fact_order_item
                     .groupBy("order_id")
                     .agg(
                         F.count("order_item_id").alias("items_count"),
                         F.sum("price").alias("sum_price"),
                         F.sum("freight_value").alias("sum_freight")
                     )
    )
    
    # Aggregate payments
    payment_agg = (silver_payment
                  .groupBy("order_id")
                  .agg(
                      F.sum("payment_value").alias("payment_total"),
                      F.max("payment_installments").alias("payment_installments_max"),
                      F.first("payment_type").alias("primary_payment_type")
                  )
    )
    
    # Calculate delivery metrics
    df = (silver_order
          .join(order_item_agg, "order_id", "left")
          .join(payment_agg, "order_id", "left")
          .withColumn("delivered_days", 
                     F.datediff("order_delivered_customer_date", "order_purchase_timestamp"))
          .withColumn("delivered_on_time",
                     F.when(F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date"), True)
                     .otherwise(False))
          .withColumn("is_canceled",
                     F.when(F.col("order_status") == "canceled", True)
                     .otherwise(False))
          .withColumn("full_date", F.col("order_purchase_timestamp").cast("date"))
          .select(
              "order_id",
              "customer_id",
              "full_date",
              "items_count",
              "sum_price",
              "sum_freight", 
              "payment_total",
              "payment_installments_max",
              "primary_payment_type",
              "delivered_days",
              "delivered_on_time",
              "is_canceled"
          )
    )
    log_shape(context, df, "fact_order")
    return Output(df)

@asset(
    ins={"silver_payment": AssetIn(key_prefix=["silver", "payment"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "factpayment"],
    group_name=LAYER,
)
def fact_payment(context, silver_payment: DataFrame):
    """Fact table with grain: 1 row per order_id + payment_sequential"""
    df = (silver_payment
          .select(
              "order_id",
              "payment_sequential",
              "payment_type",
              "payment_installments",
              "payment_value"
          )
    )
    log_shape(context, df, "fact_payment")
    return Output(df)

@asset(
    ins={"silver_order_review": AssetIn(key_prefix=["silver", "orderreview"])},
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "factreview"],
    group_name=LAYER,
)
def fact_review(context, silver_order_review: DataFrame):
    """Fact table with grain: 1 row per review_id"""
    df = (silver_order_review
          .select(
              "review_id",
              "order_id",
              "review_score",
              "review_creation_date",
              "review_answer_timestamp"
          )
    )
    log_shape(context, df, "fact_review")
    return Output(df)