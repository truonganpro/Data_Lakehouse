from dagster import asset, AssetIn, Output
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window

COMPUTE_KIND = "SparkSQL"
LAYER = "platinum"

def log_shape(context, df: DataFrame, name: str):
    context.log.info(f"{name} rows={df.count()} cols={len(df.columns)}")

# --------------------
# PLATINUM DATAMARTS FOR BI
# --------------------

@asset(
    ins={
        "fact_order_item": AssetIn(key_prefix=["gold", "factorderitem"]),
        "dim_product": AssetIn(key_prefix=["gold", "dimproduct"]),
        "dim_product_category": AssetIn(key_prefix=["gold", "dimproductcategory"]),
        "dim_date": AssetIn(key_prefix=["gold", "dimdate"]),
    },
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "dmsalesmonthlycategory"],
    group_name=LAYER,
)
def dm_sales_monthly_category(context, fact_order_item, dim_product, dim_product_category, dim_date):
    """Datamart: Monthly sales by category"""
    df = (fact_order_item
          .join(dim_product, "product_id", "left")
          .join(dim_product_category, dim_product["product_category_name"] == dim_product_category["product_category_name"], "left")
          .join(dim_date, "full_date", "left")
          .groupBy("year_month", "product_category_name_english")
          .agg(
              F.sum("price").alias("gmv"),
              F.countDistinct("order_id").alias("orders"),
              F.count("*").alias("units"),
              (F.sum("price") / F.countDistinct("order_id")).alias("aov")
          )
          .orderBy("year_month", F.desc("gmv"))
    )
    log_shape(context, df, "dm_sales_monthly_category")
    return Output(df)

@asset(
    ins={
        "fact_order_item": AssetIn(key_prefix=["gold", "factorderitem"]),
        "fact_order": AssetIn(key_prefix=["gold", "factorder"]),
        "fact_review": AssetIn(key_prefix=["gold", "factreview"]),
        "dim_seller": AssetIn(key_prefix=["gold", "dimseller"]),
    },
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "dmsellerkpi"],
    group_name=LAYER,
)
def dm_seller_kpi(context, fact_order_item, fact_order, fact_review, dim_seller):
    """Datamart: Seller KPIs"""
    
    # Seller order metrics
    seller_orders = (fact_order_item
                    .groupBy("seller_id")
                    .agg(
                        F.sum("price").alias("gmv"),
                        F.countDistinct("order_id").alias("orders"),
                        F.count("*").alias("units")
                    )
    )
    
    # Seller review metrics
    seller_reviews = (fact_review
                     .join(fact_order_item.select("order_id", "seller_id"), "order_id", "left")
                     .groupBy("seller_id")
                     .agg(F.avg("review_score").alias("avg_review_score"))
    )
    
    # Seller delivery metrics
    seller_delivery = (fact_order
                      .join(fact_order_item.select("order_id", "seller_id"), "order_id", "left")
                      .groupBy("seller_id")
                      .agg(
                          F.avg(F.when(F.col("delivered_on_time") == True, 1).otherwise(0)).alias("on_time_rate"),
                          F.avg(F.when(F.col("is_canceled") == True, 1).otherwise(0)).alias("cancel_rate")
                      )
    )
    
    df = (dim_seller
          .join(seller_orders, "seller_id", "left")
          .join(seller_reviews, "seller_id", "left")
          .join(seller_delivery, "seller_id", "left")
          .orderBy(F.desc("gmv"))
    )
    log_shape(context, df, "dm_seller_kpi")
    return Output(df)

@asset(
    ins={
        "fact_order_item": AssetIn(key_prefix=["gold", "factorderitem"]),
        "dim_customer": AssetIn(key_prefix=["gold", "dimcustomer"]),
        "dim_date": AssetIn(key_prefix=["gold", "dimdate"]),
    },
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "dmcustomerlifecycle"],
    group_name=LAYER,
)
def dm_customer_lifecycle(context, fact_order_item, dim_customer, dim_date):
    """Datamart: Customer lifecycle analysis"""
    
    # First purchase month per customer
    first_purchase = (fact_order_item
                     .groupBy("customer_id")
                     .agg(F.min("full_date").alias("first_purchase_date"))
                     .withColumn("first_purchase_month", F.date_format("first_purchase_date", "yyyy-MM"))
    )
    
    # Monthly activity per customer
    monthly_activity = (fact_order_item
                       .join(dim_date, "full_date", "left")
                       .groupBy("customer_id", "year_month")
                       .agg(
                           F.countDistinct("order_id").alias("orders"),
                           F.sum("price").alias("gmv")
                       )
    )
    
    df = (monthly_activity
          .join(first_purchase, "customer_id", "left")
          .join(dim_customer, "customer_id", "left")
          .withColumn("cohort_month", F.col("first_purchase_month"))
          .select(
              "customer_id",
              "customer_unique_id",
              "year_month",
              "cohort_month", 
              "orders",
              "gmv"
          )
          .orderBy("customer_id", "year_month")
    )
    log_shape(context, df, "dm_customer_lifecycle")
    return Output(df)

@asset(
    ins={
        "fact_payment": AssetIn(key_prefix=["gold", "factpayment"]),
        "fact_order": AssetIn(key_prefix=["gold", "factorder"]),
        "dim_date": AssetIn(key_prefix=["gold", "dimdate"]),
    },
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "dmpaymentmix"],
    group_name=LAYER,
)
def dm_payment_mix(context, fact_payment, fact_order, dim_date):
    """Datamart: Payment mix analysis"""
    df = (fact_payment
          .join(fact_order.select("order_id", "customer_id", "full_date"), "order_id", "left")
          .join(dim_date, "full_date", "left")
          .groupBy("year_month", "payment_type")
          .agg(
              F.countDistinct("order_id").alias("orders"),
              F.countDistinct("customer_id").alias("unique_customers"),
              F.sum("payment_value").alias("payment_total")
          )
          .orderBy("year_month", F.desc("payment_total"))
    )
    log_shape(context, df, "dm_payment_mix")
    return Output(df)

@asset(
    ins={
        "fact_order": AssetIn(key_prefix=["gold", "factorder"]),
        "dim_customer": AssetIn(key_prefix=["gold", "dimcustomer"]),
        "dim_geolocation": AssetIn(key_prefix=["gold", "dimgeolocation"]),
        "dim_date": AssetIn(key_prefix=["gold", "dimdate"]),
    },
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "dmlogisticssla"],
    group_name=LAYER,
)
def dm_logistics_sla(context, fact_order, dim_customer, dim_geolocation, dim_date):
    """Datamart: Logistics SLA analysis"""
    df = (fact_order
          .join(dim_customer.select("customer_id", "customer_zip_code_prefix"), "customer_id", "left")
          .join(dim_geolocation, dim_customer["customer_zip_code_prefix"] == dim_geolocation["geolocation_zip_code_prefix"], "left")
          .join(dim_date, "full_date", "left")
          .groupBy("year_month", "geolocation_state")
          .agg(
              F.avg("delivered_days").alias("avg_delivered_days"),
              F.avg(F.when(F.col("delivered_on_time") == True, 1).otherwise(0)).alias("on_time_rate"),
              F.sum(F.when(F.col("delivered_on_time") == False, 1).otherwise(0)).alias("late_orders")
          )
          .orderBy("year_month", F.desc("on_time_rate"))
    )
    log_shape(context, df, "dm_logistics_sla")
    return Output(df)

@asset(
    ins={
        "fact_order_item": AssetIn(key_prefix=["gold", "factorderitem"]),
        "fact_review": AssetIn(key_prefix=["gold", "factreview"]),
        "dim_product": AssetIn(key_prefix=["gold", "dimproduct"]),
        "dim_product_category": AssetIn(key_prefix=["gold", "dimproductcategory"]),
    },
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "dmproductbestsellers"],
    group_name=LAYER,
)
def dm_product_bestsellers(context, fact_order_item, fact_review, dim_product, dim_product_category):
    """Datamart: Product bestsellers analysis"""
    
    # Product metrics
    product_metrics = (fact_order_item
                      .join(dim_product, "product_id", "left")
                      .join(dim_product_category, dim_product["product_category_name"] == dim_product_category["product_category_name"], "left")
                      .groupBy("product_id", "product_category_name_english")
                      .agg(
                          F.sum("price").alias("gmv"),
                          F.count("*").alias("units"),
                          F.countDistinct("order_id").alias("orders")
                      )
    )
    
    # Product reviews
    product_reviews = (fact_review
                      .join(fact_order_item.select("order_id", "product_id"), "order_id", "left")
                      .groupBy("product_id")
                      .agg(F.avg("review_score").alias("avg_review_score"))
    )
    
    df = (product_metrics
          .join(product_reviews, "product_id", "left")
          .withColumn("rank_in_category", 
                     F.row_number().over(
                         Window.partitionBy("product_category_name_english")
                         .orderBy(F.desc("gmv"))
                     ))
          .orderBy("product_category_name_english", "rank_in_category")
    )
    log_shape(context, df, "dm_product_bestsellers")
    return Output(df)

@asset(
    ins={
        "fact_order_item": AssetIn(key_prefix=["gold", "factorderitem"]),
        "dim_product": AssetIn(key_prefix=["gold", "dimproduct"]),
        "dim_product_category": AssetIn(key_prefix=["gold", "dimproductcategory"]),
    },
    io_manager_key="spark_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=[LAYER, "dmcategorypricebands"],
    group_name=LAYER,
)
def dm_category_price_bands(context, fact_order_item, dim_product, dim_product_category):
    """Datamart: Category price bands analysis"""
    df = (fact_order_item
          .join(dim_product, "product_id", "left")
          .join(dim_product_category, dim_product["product_category_name"] == dim_product_category["product_category_name"], "left")
          .withColumn("price_band", 
                     F.when(F.col("price") < 50, "Under $50")
                     .when(F.col("price") < 100, "$50-$100")
                     .when(F.col("price") < 200, "$100-$200")
                     .when(F.col("price") < 500, "$200-$500")
                     .otherwise("Over $500"))
          .groupBy("product_category_name_english", "price_band")
          .agg(
              F.count("*").alias("order_items"),
              F.countDistinct("order_id").alias("orders"),
              F.sum("price").alias("total_gmv"),
              F.avg("price").alias("avg_price")
          )
          .orderBy("product_category_name_english", "price_band")
    )
    log_shape(context, df, "dm_category_price_bands")
    return Output(df)