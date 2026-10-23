-- ============================================================================
--  shopdb: documented SQL queries (22AIE303 Database Management Systems)
--
--  Run after 01_schema.sql, 02_seed.sql and 03_roles.sql, e.g.
--      psql -U postgres -d shopdb -f database/queries.sql
--  or open this file in the pgAdmin Query Tool (connected to shopdb) and run
--  one query at a time (select it, press F5).
--
--  The outputs of every query on a freshly seeded database are recorded in
--  database/sample_outputs.txt.
--
--  Sections
--    A. Schema inspection (tables, columns, keys, constraints, triggers)
--    B. Multi-table joins
--    C. GROUP BY, aggregate functions and HAVING
--    D. Subqueries (scalar, IN, EXISTS / NOT EXISTS, correlated, derived tables)
--    E. Common table expressions and window functions
--    F. Views
--    G. Transactions and triggers (rolled back, so the data is unchanged)
-- ============================================================================


-- ============================================================ A. inspection

\echo '== A1. Tables in the database'
-- Purpose: list the 7 relations of the schema.
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
ORDER BY table_name;

\echo '== A2. Number of rows in every table'
-- Purpose: show the size of the seeded database.
SELECT 'categories' AS table_name, COUNT(*) AS rows FROM categories
UNION ALL SELECT 'products',             COUNT(*) FROM products
UNION ALL SELECT 'customers',            COUNT(*) FROM customers
UNION ALL SELECT 'orders',               COUNT(*) FROM orders
UNION ALL SELECT 'order_items',          COUNT(*) FROM order_items
UNION ALL SELECT 'payments',             COUNT(*) FROM payments
UNION ALL SELECT 'order_status_history', COUNT(*) FROM order_status_history;

\echo '== A3. Primary keys, foreign keys, unique and check constraints'
-- Purpose: prove every integrity constraint is declared in the DBMS.
SELECT tc.table_name, tc.constraint_type, tc.constraint_name
FROM information_schema.table_constraints tc
WHERE tc.table_schema = 'public'
  AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK')
  AND tc.constraint_name NOT LIKE '%_not_null'
ORDER BY tc.table_name, tc.constraint_type, tc.constraint_name;

\echo '== A4. Foreign keys and the tables they reference (with delete rule)'
SELECT kcu.table_name, kcu.column_name,
       ccu.table_name AS references_table, ccu.column_name AS references_column,
       rc.delete_rule
FROM information_schema.referential_constraints rc
JOIN information_schema.key_column_usage kcu
  ON kcu.constraint_name = rc.constraint_name
JOIN information_schema.constraint_column_usage ccu
  ON ccu.constraint_name = rc.unique_constraint_name
ORDER BY kcu.table_name, kcu.column_name;

\echo '== A5. Triggers'
SELECT trigger_name, event_object_table AS on_table,
       action_timing AS timing, string_agg(event_manipulation, ', ') AS events
FROM information_schema.triggers
GROUP BY trigger_name, event_object_table, action_timing
ORDER BY trigger_name;


-- ============================================================ B. joins

\echo '== B1. INNER JOIN over 5 tables: the lines of order 1'
-- Purpose: rebuild an order from its normalized parts.
SELECT o.order_id, c.name AS customer, p.name AS product, cat.name AS category,
       oi.quantity, oi.unit_price, oi.quantity * oi.unit_price AS line_total
FROM orders o
JOIN customers   c   ON c.customer_id   = o.customer_id
JOIN order_items oi  ON oi.order_id     = o.order_id
JOIN products    p   ON p.product_id    = oi.product_id
JOIN categories  cat ON cat.category_id = p.category_id
WHERE o.order_id = 1
ORDER BY oi.order_item_id;

\echo '== B2. JOIN with payments and the latest status change of order 1'
SELECT o.order_id, o.status, o.total_amount, pay.method, pay.status AS payment_status,
       h.old_status, h.new_status, h.changed_at
FROM orders o
JOIN payments pay ON pay.order_id = o.order_id
JOIN order_status_history h ON h.order_id = o.order_id
WHERE o.order_id = 1
ORDER BY h.changed_at;

\echo '== B3. LEFT JOIN: every category with its number of products'
-- Purpose: LEFT JOIN keeps categories even if they had no products.
SELECT cat.name AS category, COUNT(p.product_id) AS products,
       MIN(p.price) AS cheapest, MAX(p.price) AS most_expensive
FROM categories cat
LEFT JOIN products p ON p.category_id = cat.category_id
GROUP BY cat.name
ORDER BY cat.name;


-- ============================================================ C. GROUP BY

\echo '== C1. Revenue, orders and units sold per category'
SELECT cat.name AS category,
       COUNT(DISTINCT oi.order_id)      AS orders,
       SUM(oi.quantity)                 AS units,
       SUM(oi.quantity * oi.unit_price) AS revenue
FROM order_items oi
JOIN products p     ON p.product_id = oi.product_id
JOIN categories cat ON cat.category_id = p.category_id
GROUP BY cat.name
ORDER BY revenue DESC;

\echo '== C2. Orders and average order value per status'
SELECT status, COUNT(*) AS orders, ROUND(AVG(total_amount), 2) AS avg_order_value
FROM orders
GROUP BY status
ORDER BY orders DESC;

\echo '== C3. Payment methods that collected more than 33 lakh (HAVING)'
-- Purpose: HAVING filters groups after aggregation (WHERE cannot do this).
SELECT method, COUNT(*) AS payments, SUM(amount) AS total_amount
FROM payments
GROUP BY method
HAVING SUM(amount) > 3300000
ORDER BY total_amount DESC;

\echo '== C4. Top 5 cities by revenue'
SELECT c.city, COUNT(o.order_id) AS orders, SUM(o.total_amount) AS revenue
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.city
ORDER BY revenue DESC
LIMIT 5;


-- ============================================================ D. subqueries

\echo '== D1. Customers whose total spending is above the average (subquery in HAVING)'
SELECT c.customer_id, c.name, c.city, SUM(o.total_amount) AS total_spent
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.name, c.city
HAVING SUM(o.total_amount) > (
    SELECT AVG(t.spent)
    FROM (SELECT SUM(total_amount) AS spent FROM orders GROUP BY customer_id) t)
ORDER BY total_spent DESC
LIMIT 5;

\echo '== D2. Orders worth more than the average order (scalar subquery)'
SELECT COUNT(*) AS orders_above_average,
       (SELECT ROUND(AVG(total_amount), 2) FROM orders) AS average_order_value
FROM orders
WHERE total_amount > (SELECT AVG(total_amount) FROM orders);

\echo '== D3. Customers who bought something from Electronics (IN subquery)'
SELECT COUNT(*) AS electronics_customers
FROM customers
WHERE customer_id IN (
    SELECT o.customer_id
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    JOIN products p     ON p.product_id = oi.product_id
    JOIN categories cat ON cat.category_id = p.category_id
    WHERE cat.name = 'Electronics');

\echo '== D4. Customers with no delivered order (correlated NOT EXISTS)'
SELECT COUNT(*) AS customers_without_delivery
FROM customers c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o
    WHERE o.customer_id = c.customer_id AND o.status = 'delivered');

\echo '== D5. Each product compared with the average price of its category (correlated subquery)'
SELECT p.name, cat.name AS category, p.price,
       ROUND((SELECT AVG(p2.price) FROM products p2 WHERE p2.category_id = p.category_id), 2) AS category_avg
FROM products p
JOIN categories cat ON cat.category_id = p.category_id
WHERE p.price > (SELECT AVG(p2.price) FROM products p2 WHERE p2.category_id = p.category_id)
ORDER BY cat.name, p.price DESC;


-- ============================================================ E. CTE + windows

\echo '== E1. Best-selling product in each category (window function RANK)'
SELECT category, product, units
FROM (
    SELECT cat.name AS category, p.name AS product, SUM(oi.quantity) AS units,
           RANK() OVER (PARTITION BY cat.name ORDER BY SUM(oi.quantity) DESC) AS rnk
    FROM order_items oi
    JOIN products p     ON p.product_id = oi.product_id
    JOIN categories cat ON cat.category_id = p.category_id
    GROUP BY cat.name, p.name) ranked
WHERE rnk = 1
ORDER BY units DESC;

\echo '== E2. Top customer in each of the first 5 cities (CTE + DENSE_RANK)'
WITH spend AS (
    SELECT c.city, c.name, SUM(o.total_amount) AS total_spent
    FROM customers c JOIN orders o ON o.customer_id = c.customer_id
    GROUP BY c.city, c.name),
ranked AS (
    SELECT city, name, total_spent,
           DENSE_RANK() OVER (PARTITION BY city ORDER BY total_spent DESC) AS rnk
    FROM spend)
SELECT city, name, total_spent
FROM ranked
WHERE rnk = 1
ORDER BY city
LIMIT 5;

\echo '== E3. Running total of one customer''s spending (window SUM)'
SELECT o.order_id, o.order_date::date AS day, o.total_amount,
       SUM(o.total_amount) OVER (ORDER BY o.order_date, o.order_id) AS running_total
FROM orders o
WHERE o.customer_id = 149
ORDER BY o.order_date, o.order_id;


-- ============================================================ F. views

\echo '== F1. View: one row per order with customer and item count'
CREATE OR REPLACE VIEW order_summary AS
SELECT o.order_id, o.order_date, o.status, o.total_amount,
       c.name AS customer, c.city,
       COUNT(oi.order_item_id) AS items
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
LEFT JOIN order_items oi ON oi.order_id = o.order_id
GROUP BY o.order_id, c.name, c.city;

SELECT * FROM order_summary ORDER BY order_id LIMIT 5;


-- ============================================================ G. transactions

\echo '== G1. Transaction with triggers: place an order, inspect it, then ROLLBACK'
-- Purpose: the trigger fills total_amount and the status history inside the
-- same transaction; ROLLBACK undoes everything (atomicity).
BEGIN;
  SELECT product_id, name, price, stock FROM products
   WHERE product_id IN (1, 2) ORDER BY product_id FOR UPDATE;   -- row locks
  INSERT INTO orders (customer_id, status, total_amount) VALUES (149, 'pending', 0);
  INSERT INTO order_items (order_id, product_id, quantity, unit_price)
       VALUES (currval('orders_order_id_seq'), 1, 1, 2499.00),
              (currval('orders_order_id_seq'), 2, 1, 4999.00);
  UPDATE products SET stock = stock - 1 WHERE product_id IN (1, 2);
  SELECT order_id, status, total_amount FROM orders WHERE order_id = currval('orders_order_id_seq');
  SELECT old_status, new_status FROM order_status_history WHERE order_id = currval('orders_order_id_seq');
ROLLBACK;

\echo '== G2. After ROLLBACK the order is gone and the row counts are unchanged'
SELECT COUNT(*) AS orders FROM orders;

-- Constraint violations (run one at a time; each is rejected by the DBMS):
--   UPDATE products SET stock = -1 WHERE product_id = 1;            -- CHECK (stock >= 0)
--   INSERT INTO orders (customer_id, status, total_amount)
--        VALUES (99999, 'pending', 0);                              -- FOREIGN KEY
--   UPDATE orders SET status = 'lost' WHERE order_id = 1;           -- CHECK (status IN ...)
--   INSERT INTO categories (name) VALUES ('Books');                 -- UNIQUE (name)
