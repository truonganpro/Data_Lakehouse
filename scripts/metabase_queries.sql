-- =====================================================
-- METABASE SAMPLE QUERIES - Brazilian E-commerce
-- =====================================================

-- 1. OVERVIEW METRICS
-- =====================================================

-- Total Customers
SELECT COUNT(DISTINCT customer_id) as total_customers
FROM lakehouse.bronze.customer;

-- Total Orders
SELECT COUNT(*) as total_orders
FROM lakehouse.bronze."order";

-- Total Revenue
SELECT ROUND(SUM(payment_value), 2) as total_revenue
FROM lakehouse.bronze.payment;

-- Average Order Value
SELECT ROUND(AVG(payment_value), 2) as avg_order_value
FROM lakehouse.bronze.payment;


-- 2. CUSTOMER ANALYTICS
-- =====================================================

-- Customers by State (Top 10)
SELECT 
    customer_state,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM lakehouse.bronze.customer
GROUP BY customer_state
ORDER BY customer_count DESC
LIMIT 10;

-- Customers by City (Top 20)
SELECT 
    customer_city,
    customer_state,
    COUNT(*) as customer_count
FROM lakehouse.bronze.customer
GROUP BY customer_city, customer_state
ORDER BY customer_count DESC
LIMIT 20;


-- 3. ORDER ANALYTICS
-- =====================================================

-- Orders by Status
SELECT 
    order_status,
    COUNT(*) as order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM lakehouse.bronze."order"
GROUP BY order_status
ORDER BY order_count DESC;

-- Orders by Month
SELECT 
    DATE_FORMAT(CAST(order_purchase_timestamp AS TIMESTAMP), '%Y-%m') as month,
    COUNT(*) as order_count,
    COUNT(DISTINCT customer_id) as unique_customers
FROM lakehouse.bronze."order"
GROUP BY DATE_FORMAT(CAST(order_purchase_timestamp AS TIMESTAMP), '%Y-%m')
ORDER BY month;

-- Orders by Day of Week
SELECT 
    DAY_OF_WEEK(CAST(order_purchase_timestamp AS TIMESTAMP)) as day_of_week,
    CASE DAY_OF_WEEK(CAST(order_purchase_timestamp AS TIMESTAMP))
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END as day_name,
    COUNT(*) as order_count
FROM lakehouse.bronze."order"
GROUP BY DAY_OF_WEEK(CAST(order_purchase_timestamp AS TIMESTAMP))
ORDER BY day_of_week;


-- 4. PRODUCT ANALYTICS
-- =====================================================

-- Top 20 Product Categories by Revenue
SELECT 
    p.product_category_name,
    COUNT(DISTINCT oi.order_id) as order_count,
    COUNT(oi.product_id) as item_count,
    ROUND(SUM(oi.price), 2) as revenue,
    ROUND(AVG(oi.price), 2) as avg_price
FROM lakehouse.bronze.order_item oi
JOIN lakehouse.bronze.product p ON oi.product_id = p.product_id
WHERE p.product_category_name IS NOT NULL
GROUP BY p.product_category_name
ORDER BY revenue DESC
LIMIT 20;

-- Product Category Distribution
SELECT 
    product_category_name,
    COUNT(*) as product_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM lakehouse.bronze.product
WHERE product_category_name IS NOT NULL
GROUP BY product_category_name
ORDER BY product_count DESC
LIMIT 15;


-- 5. PAYMENT ANALYTICS
-- =====================================================

-- Payment Methods Distribution
SELECT 
    payment_type,
    COUNT(*) as transaction_count,
    ROUND(SUM(payment_value), 2) as total_amount,
    ROUND(AVG(payment_value), 2) as avg_amount,
    ROUND(SUM(payment_value) * 100.0 / SUM(SUM(payment_value)) OVER (), 2) as revenue_percentage
FROM lakehouse.bronze.payment
GROUP BY payment_type
ORDER BY total_amount DESC;

-- Payment Installments Analysis
SELECT 
    payment_installments,
    COUNT(*) as transaction_count,
    ROUND(SUM(payment_value), 2) as total_amount,
    ROUND(AVG(payment_value), 2) as avg_amount
FROM lakehouse.bronze.payment
GROUP BY payment_installments
ORDER BY payment_installments;


-- 6. REVIEW ANALYTICS
-- =====================================================

-- Review Score Distribution
SELECT 
    review_score,
    COUNT(*) as review_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM lakehouse.bronze.order_review
GROUP BY review_score
ORDER BY review_score DESC;

-- Average Review Score by Month
SELECT 
    DATE_FORMAT(CAST(review_creation_date AS TIMESTAMP), '%Y-%m') as month,
    ROUND(AVG(review_score), 2) as avg_score,
    COUNT(*) as review_count
FROM lakehouse.bronze.order_review
GROUP BY DATE_FORMAT(CAST(review_creation_date AS TIMESTAMP), '%Y-%m')
ORDER BY month;


-- 7. SELLER ANALYTICS
-- =====================================================

-- Top Sellers by Order Count
SELECT 
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT oi.order_id) as order_count,
    COUNT(oi.product_id) as item_count,
    ROUND(SUM(oi.price), 2) as revenue
FROM lakehouse.bronze.order_item oi
JOIN lakehouse.bronze.seller s ON oi.seller_id = s.seller_id
GROUP BY s.seller_id, s.seller_city, s.seller_state
ORDER BY revenue DESC
LIMIT 20;

-- Sellers by State
SELECT 
    seller_state,
    COUNT(*) as seller_count
FROM lakehouse.bronze.seller
GROUP BY seller_state
ORDER BY seller_count DESC;


-- 8. LOGISTICS ANALYTICS
-- =====================================================

-- Delivery Performance
SELECT 
    COUNT(*) as total_orders,
    COUNT(CASE WHEN order_delivered_customer_date IS NOT NULL THEN 1 END) as delivered_orders,
    ROUND(COUNT(CASE WHEN order_delivered_customer_date IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as delivery_rate,
    ROUND(AVG(CAST(order_delivered_customer_date AS TIMESTAMP) - CAST(order_purchase_timestamp AS TIMESTAMP)), 2) as avg_delivery_days
FROM lakehouse.bronze."order"
WHERE order_status = 'delivered';


-- 9. COHORT ANALYSIS
-- =====================================================

-- Monthly Cohort
SELECT 
    DATE_FORMAT(CAST(order_purchase_timestamp AS TIMESTAMP), '%Y-%m') as cohort_month,
    COUNT(DISTINCT customer_id) as new_customers,
    COUNT(*) as orders,
    ROUND(SUM(p.payment_value), 2) as revenue
FROM lakehouse.bronze."order" o
LEFT JOIN lakehouse.bronze.payment p ON o.order_id = p.order_id
GROUP BY DATE_FORMAT(CAST(order_purchase_timestamp AS TIMESTAMP), '%Y-%m')
ORDER BY cohort_month;


-- 10. COMBINED ANALYSIS
-- =====================================================

-- Complete Order Details
SELECT 
    o.order_id,
    o.customer_id,
    c.customer_city,
    c.customer_state,
    o.order_status,
    o.order_purchase_timestamp,
    COUNT(oi.product_id) as item_count,
    ROUND(SUM(p.payment_value), 2) as order_value,
    MAX(r.review_score) as review_score
FROM lakehouse.bronze."order" o
JOIN lakehouse.bronze.customer c ON o.customer_id = c.customer_id
LEFT JOIN lakehouse.bronze.order_item oi ON o.order_id = oi.order_id
LEFT JOIN lakehouse.bronze.payment p ON o.order_id = p.order_id
LEFT JOIN lakehouse.bronze.order_review r ON o.order_id = r.order_id
GROUP BY o.order_id, o.customer_id, c.customer_city, c.customer_state, o.order_status, o.order_purchase_timestamp
ORDER BY o.order_purchase_timestamp DESC
LIMIT 100;

