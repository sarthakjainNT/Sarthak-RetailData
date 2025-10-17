-- first step then all analysis queries
use retail_analysis_db;

-- Customer Segmentation (by Age, Gender, Income, Segment)
-- Age group distribution
SELECT
  CASE
     WHEN age BETWEEN 10 AND 20 THEN 'Teen'
     WHEN age BETWEEN 21 AND 35 THEN 'Young Adult'
     WHEN age BETWEEN 36 AND 50 THEN 'Adult'
     ELSE 'Senior'
  END AS age_group,
  COUNT(DISTINCT c.customer_id) AS customer_count
FROM silver_customer c
JOIN silver_retail r ON c.customer_key = r.customer_key
GROUP BY age_group;

-- Gender split
SELECT gender, COUNT(*) AS customers
FROM silver_customer
GROUP BY gender;

-- Income Category Split
SELECT income, COUNT(*) AS customers
FROM silver_customer
GROUP BY income;

-- Segment Analysis
SELECT customer_segment, COUNT(*) AS customers
FROM silver_customer
GROUP BY customer_segment;

-- Sales Trends (Daily, Monthly, Yearly)
-- Daily Sales
SELECT d.date, SUM(r.total_amount) AS total_sales
FROM silver_retail r
JOIN silver_date d ON r.date_id = d.date_key
GROUP BY d.date
ORDER BY d.date;

-- Monthly Sales
SELECT d.year, d.month, SUM(r.total_amount) AS total_sales
FROM silver_retail r
JOIN silver_date d ON r.date_id = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Yearly Sales
SELECT d.year, SUM(r.total_amount) AS total_sales
FROM silver_retail r
JOIN silver_date d ON r.date_id = d.date_key
GROUP BY d.year
ORDER BY d.year;

-- seasonal and regional Sales
SELECT d.year, d.month, c.country, SUM(r.total_amount) AS total_sales
FROM silver_retail r
JOIN silver_date d ON r.date_id = d.date_key
JOIN silver_customer c ON r.customer_key = c.customer_key
GROUP BY d.year, d.month, c.country
ORDER BY d.year, d.month, total_sales DESC;

--product performance and brand loyalty
SELECT 
    p.product_category,
    p.product_brand,
    COUNT(r.order_status = 'Delivered') AS total_orders,
    SUM(r.amount) AS total_sales,
    AVG(r.ratings) AS avg_rating
FROM silver_retail r
JOIN silver_product p ON r.product_key = p.product_key
GROUP BY p.product_category, p.product_brand
ORDER BY total_sales DESC;

-- Top 5 Products by Sales
SELECT p.products, p.product_category, p.product_brand, SUM(r.total_amount) AS total_sales
FROM silver_retail r
JOIN silver_product p ON r.product_key = p.product_key
GROUP BY p.products,p.product_category, p.product_brand
ORDER BY total_sales DESC
LIMIT 5;

-- Bottom 5 Products by Sales
SELECT p.product_category, p.product_brand, SUM(r.amount) AS total_sales
FROM silver_retail r
JOIN silver_product p ON r.product_key = p.product_key
GROUP BY p.product_category, p.product_brand
ORDER BY total_sales ASC
LIMIT 5;

-- Shipping method vs Delivery performance
SELECT s.shipping_method, r.order_status, COUNT(*) AS order_count
FROM silver_retail r
JOIN silver_shipping s ON r.shipping_id = s.shipping_id
GROUP BY s.shipping_method, r.order_status
ORDER BY s.shipping_method, order_count DESC;

-- compare delivery success and failure
SELECT 
    CASE 
        WHEN order_status = 'Delivered' THEN 'Success'
        ELSE 'Failure'
    END AS delivery_result,
    COUNT(*) AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM silver_retail
GROUP BY 
    CASE 
        WHEN order_status = 'Delivered' THEN 'Success'
        ELSE 'Failure'
    END;

-- Payment method and order reliability 
SELECT p.payment_method,
       SUM(CASE WHEN r.order_status = 'Delivered' THEN 1 ELSE 0 END) AS successful_orders,
       SUM(CASE WHEN r.order_status != 'Delivered' THEN 1 ELSE 0 END) AS failed_orders,
       ROUND(SUM(CASE WHEN r.order_status = 'Delivered' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS reliability_percent
FROM silver_retail r
JOIN silver_payment p ON r.payment_id = p.payment_id
GROUP BY p.payment_method
ORDER BY reliability_percent DESC;

--customer retention and loyalty
SELECT c.customer_id,
       COUNT(r.transaction_id) AS total_orders,
       SUM(r.amount) AS total_spend,
       MAX(r.transaction_date) AS last_purchase,
       CASE 
           WHEN COUNT(r.transaction_id) >= 10 AND SUM(r.amount) > 10000 THEN 'VIP'
           WHEN COUNT(r.transaction_id) >= 3 THEN 'Regular'
           ELSE 'At Risk'
       END AS loyalty_segment
FROM silver_retail r
JOIN silver_customer c ON r.customer_key = c.customer_key
GROUP BY c.customer_id;


-- customer retention and loyalty with name and order success
SELECT 
    c.customer_id,
    c.name,
    COUNT(r.order_status = 'Delivered') AS total_purchases,
    SUM(r.amount) AS total_spent,
    MAX(r.transaction_date) AS last_purchase_date,
    CASE 
        WHEN SUM(r.amount) >= 5000 THEN 'VIP'
        WHEN SUM(r.amount) BETWEEN 1000 AND 4999 THEN 'Regular'
        ELSE 'At Risk'
    END AS loyalty_segment
FROM silver_customer c
JOIN silver_retail r ON c.customer_key = r.customer_key
GROUP BY c.customer_id, c.name
ORDER BY total_spent DESC;

-- top 5 revenue 
SELECT 
    p.products,
    p.product_category,
    SUM(r.amount) AS revenue
FROM silver_retail r
JOIN silver_product p ON r.product_key = p.product_key
GROUP BY p.product_id, p.products, p.product_category
ORDER BY revenue DESC
LIMIT 5;

--bottom 5 revenue
SELECT 
    p.products,
    p.product_category,
    SUM(r.amount) AS revenue
FROM silver_retail r
JOIN silver_product p ON r.product_key = p.product_key
GROUP BY p.product_id, p.products, p.product_category
ORDER BY revenue ASC
LIMIT 5;