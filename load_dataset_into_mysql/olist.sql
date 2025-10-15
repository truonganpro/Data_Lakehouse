SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS sellers;
DROP TABLE IF EXISTS geolocation;
DROP TABLE IF EXISTS order_reviews;
DROP TABLE IF EXISTS product_category_name_translation;

SET FOREIGN_KEY_CHECKS = 1;
-- Tạo cơ sở dữ liệu nếu chưa tồn tại
CREATE DATABASE IF NOT EXISTS brazillian_ecommerce;
USE brazillian_ecommerce;

DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS geolocation;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS order_payments;
DROP TABLE IF EXISTS order_reviews;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS sellers;
DROP TABLE IF EXISTS product_category_name_translation;

CREATE TABLE customers (
  customer_id VARCHAR(50) NOT NULL,
  customer_unique_id VARCHAR(50) NOT NULL,
  customer_zip_code_prefix INT,
  customer_city VARCHAR(50),
  customer_state VARCHAR(50),
  PRIMARY KEY (customer_id)
);

CREATE TABLE geolocation (
  geolocation_zip_code_prefix INT NOT NULL,
  geolocation_lat FLOAT,
  geolocation_lng FLOAT,
  geolocation_city VARCHAR(50),
  geolocation_state VARCHAR(50),
  PRIMARY KEY (geolocation_zip_code_prefix)
);

CREATE TABLE order_items (
  order_id VARCHAR(50) NOT NULL,
  order_item_id INT NOT NULL,
  product_id VARCHAR(50),
  seller_id VARCHAR(50),
  shipping_limit_date DATETIME,
  price FLOAT,
  freight_value FLOAT,
  PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE order_payments (
  order_id VARCHAR(50) NOT NULL,
  payment_sequential INT NOT NULL,
  payment_type VARCHAR(50),
  payment_installments INT,
  payment_value FLOAT,
  PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE order_reviews (
  review_id VARCHAR(50) NOT NULL,
  order_id VARCHAR(50),
  review_score INT,
  review_comment_title TEXT,
  review_comment_message TEXT,
  review_creation_date DATETIME,
  review_answer_timestamp DATETIME,
  PRIMARY KEY (review_id)
);

CREATE TABLE orders (
  order_id VARCHAR(50) NOT NULL,
  customer_id VARCHAR(50),
  order_status VARCHAR(50),
  order_purchase_timestamp DATETIME,
  order_approved_at DATETIME,
  order_delivered_carrier_date DATETIME,
  order_delivered_customer_date DATETIME,
  order_estimated_delivery_date DATETIME,
  PRIMARY KEY (order_id)
);

CREATE TABLE products (
  product_id VARCHAR(50) NOT NULL,
  product_category_name VARCHAR(100),
  product_name_lenght INT,
  product_description_lenght INT,
  product_photos_qty INT,
  product_weight_g INT,
  product_length_cm INT,
  product_height_cm INT,
  product_width_cm INT,
  PRIMARY KEY (product_id)
);

CREATE TABLE sellers (
  seller_id VARCHAR(50) NOT NULL,
  seller_zip_code_prefix INT,
  seller_city VARCHAR(50),
  seller_state VARCHAR(50),
  PRIMARY KEY (seller_id)
);

CREATE TABLE product_category_name_translation (
  product_category_name VARCHAR(100) NOT NULL,
  product_category_name_english VARCHAR(100),
  PRIMARY KEY (product_category_name)
);