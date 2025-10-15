# 📊 Dataset Information

## Brazilian E-commerce Dataset

This project uses the **Brazilian E-commerce Public Dataset** from Olist.

### 📥 Download Instructions

1. **Visit the official dataset page**: [Kaggle - Brazilian E-commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

2. **Download all CSV files** and place them in the `brazilian-ecommerce/` directory:
   ```
   brazilian-ecommerce/
   ├── olist_customers_dataset.csv
   ├── olist_geolocation_dataset.csv
   ├── olist_order_items_dataset.csv
   ├── olist_order_payments_dataset.csv
   ├── olist_order_reviews_dataset.csv
   ├── olist_orders_dataset.csv
   ├── olist_products_dataset.csv
   ├── olist_sellers_dataset.csv
   └── product_category_name_translation.csv
   ```

3. **Alternative download script**:
   ```bash
   # Create dataset directory
   mkdir -p brazilian-ecommerce
   
   # Download using Kaggle API (requires kaggle account)
   kaggle datasets download -d olistbr/brazilian-ecommerce -p brazilian-ecommerce/
   unzip brazilian-ecommerce/brazilian-ecommerce.zip -d brazilian-ecommerce/
   ```

### 📋 Dataset Description

- **Total Records**: ~100K orders, 32K products, 9K sellers
- **Time Period**: 2016-2018
- **Geographic Coverage**: All Brazilian states
- **Data Quality**: Real anonymized data from Olist marketplace

### 🔧 Setup After Download

Once you have the dataset files:

1. **Start the system**:
   ```bash
   docker-compose up -d
   ```

2. **Run the ETL pipeline**:
   ```bash
   # Access Dagster UI at http://localhost:3001
   # Or run via command line
   docker exec de_dagster_daemon dagster job execute -j etl_job
   ```

3. **Verify data processing**:
   - Check MinIO buckets at http://localhost:9001
   - Query data via Trino at http://localhost:8082
   - View dashboards in Metabase at http://localhost:3000

### 📊 Data Schema

| Table | Description | Key Fields |
|-------|-------------|------------|
| `customers` | Customer information | customer_id, customer_zip_code_prefix |
| `orders` | Order details | order_id, customer_id, order_status |
| `order_items` | Order line items | order_id, product_id, seller_id, price |
| `products` | Product catalog | product_id, product_category_name |
| `sellers` | Seller information | seller_id, seller_zip_code_prefix |
| `geolocation` | Geographic data | geolocation_zip_code_prefix, city, state |
| `order_payments` | Payment information | order_id, payment_type, payment_value |
| `order_reviews` | Customer reviews | review_id, order_id, review_score |

### ⚠️ Important Notes

- Dataset files are **not included** in this repository due to size limitations
- You must download the dataset separately before running the ETL pipeline
- The system is designed to work with the exact file names and structure from the official dataset
