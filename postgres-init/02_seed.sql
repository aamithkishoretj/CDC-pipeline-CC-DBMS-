-- ============================================================================
--  Seed data: 6 categories, 24 products, 200 customers, 1200 orders
--  (~2400 order items, 1200 payments, ~3500 status-history rows).
--  Enough volume that a JOIN-heavy read against Postgres does real work,
--  so the Postgres-vs-Redis benchmark is meaningful.
--  Debezium's initial snapshot streams all of this into Kafka -> Redis.
-- ============================================================================

SELECT setseed(0.42);   -- reproducible "random" data

-- ---------------------------------------------------------------- categories
INSERT INTO categories (name, description) VALUES
 ('Electronics',  'Phones, audio and gadgets'),
 ('Computers',    'Laptops, peripherals and accessories'),
 ('Home',         'Home and kitchen essentials'),
 ('Books',        'Fiction, non-fiction and textbooks'),
 ('Fashion',      'Clothing, footwear and accessories'),
 ('Sports',       'Fitness and outdoor equipment');

-- ------------------------------------------------------------------ products
INSERT INTO products (category_id, name, description, price, stock) VALUES
 (1, 'Wireless Earbuds',          'Bluetooth 5.3, 30h battery',          2499.00, 320),
 (1, 'Smartwatch',                'AMOLED display, heart-rate sensor',   4999.00, 150),
 (1, 'Bluetooth Speaker',         'Portable, waterproof',                1899.00, 260),
 (1, 'Power Bank 20000mAh',       'Fast charging, USB-C',                1599.00, 400),
 (2, 'Wireless Mouse',            'Ergonomic, silent clicks',             799.00, 500),
 (2, 'Mechanical Keyboard',       'Hot-swappable, RGB',                  3499.00, 300),
 (2, 'USB-C Hub',                 '7-in-1, 4K HDMI',                     1299.00, 450),
 (2, '27" Monitor',               'IPS, 144Hz',                         15999.00,  80),
 (2, 'Laptop Stand',              'Aluminium, adjustable',                999.00, 350),
 (2, 'Portable SSD 1TB',          'USB 3.2, 1050MB/s',                   6499.00, 120),
 (3, 'Air Fryer',                 '4.5L, digital',                       5499.00,  90),
 (3, 'Electric Kettle',           '1.5L, stainless steel',               1199.00, 280),
 (3, 'Desk Lamp',                 'LED, 3 colour modes',                  899.00, 300),
 (3, 'Water Bottle',              'Insulated, 1L',                        599.00, 600),
 (4, 'Database System Concepts',  'Silberschatz, Korth, Sudarshan',      1150.00, 200),
 (4, 'Designing Data-Intensive Applications', 'Martin Kleppmann',        1499.00, 180),
 (4, 'Clean Code',                'Robert C. Martin',                     799.00, 220),
 (4, 'The Pragmatic Programmer',  'Hunt and Thomas',                      899.00, 210),
 (5, 'Running Shoes',             'Lightweight mesh',                    3299.00, 160),
 (5, 'Denim Jacket',              'Classic fit',                         2199.00, 140),
 (5, 'Backpack',                  '30L, laptop sleeve',                  1799.00, 260),
 (6, 'Yoga Mat',                  '6mm, anti-slip',                       899.00, 340),
 (6, 'Dumbbell Set',              '2 x 5kg',                             2299.00, 110),
 (6, 'Cricket Bat',               'English willow',                      4599.00,  70);

-- ----------------------------------------------------------------- customers
INSERT INTO customers (name, email, phone, address, city) VALUES
 ('Aditi Rao',      'aditi.rao@example.com',     '9800000001', '12 MG Road',          'Bengaluru'),
 ('Karan Mehta',    'karan.mehta@example.com',   '9800000002', '45 Linking Road',     'Mumbai'),
 ('Sara Iyer',      'sara.iyer@example.com',     '9800000003', '8 Anna Salai',        'Chennai'),
 ('Rahul Nair',     'rahul.nair@example.com',    '9800000004', '21 MG Road',          'Kochi'),
 ('Priya Sharma',   'priya.sharma@example.com',  '9800000005', '3 Connaught Place',   'Delhi'),
 ('Arjun Menon',    'arjun.menon@example.com',   '9800000006', '77 Banjara Hills',    'Hyderabad'),
 ('Neha Gupta',     'neha.gupta@example.com',    '9800000007', '19 Park Street',      'Kolkata'),
 ('Vikram Singh',   'vikram.singh@example.com',  '9800000008', '5 FC Road',           'Pune'),
 ('Ananya Krishnan','ananya.k@example.com',      '9800000009', '60 RS Puram',         'Coimbatore'),
 ('Rohan Das',      'rohan.das@example.com',     '9800000010', '14 Salt Lake',        'Kolkata');

-- 190 more generated customers (ids 11..200)
INSERT INTO customers (name, email, phone, address, city)
SELECT
    (ARRAY['Amit','Divya','Suresh','Kavya','Manoj','Lakshmi','Nikhil','Pooja',
           'Sanjay','Meera','Varun','Deepa','Harish','Sneha','Gokul','Anjali'])[1 + (g % 16)]
    || ' ' ||
    (ARRAY['Kumar','Reddy','Pillai','Joshi','Patel','Verma','Bhat','Shetty',
           'Kapoor','Rao','Nambiar','Chopra'])[1 + ((g / 16) % 12)],
    'customer' || g || '@example.com',
    '98' || lpad((10000000 + g)::text, 8, '0'),
    g || ' Example Street',
    (ARRAY['Bengaluru','Mumbai','Chennai','Delhi','Hyderabad','Pune',
           'Kolkata','Kochi','Coimbatore','Ahmedabad'])[1 + (g % 10)]
FROM generate_series(11, 200) AS g;

-- -------------------------------------------------------------------- orders
-- Insert as 'pending' with total 0; items + triggers fill in the real total.
INSERT INTO orders (customer_id, order_date, status, total_amount)
SELECT 1 + floor(random() * 200)::int,
       NOW() - (random() * INTERVAL '90 days'),
       'pending',
       0
FROM generate_series(1, 1200);

-- --------------------------------------------------------------- order_items
-- 1-3 distinct products per order; unit_price snapshots the current price.
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT o.order_id, p.product_id, 1 + floor(random() * 3)::int, p.price
FROM orders o
CROSS JOIN LATERAL (
    SELECT product_id, price
    FROM products
    ORDER BY random()
    LIMIT 1 + (o.order_id % 3)
) p;

-- ---------------------------------------------------- status life-cycle
-- Walk orders through the state machine so the history trigger records
-- realistic transitions: 80% processing, 60% shipped, 40% delivered.
UPDATE orders SET status = 'processing' WHERE order_id % 10 < 8;
UPDATE orders SET status = 'shipped'    WHERE order_id % 10 < 6;
UPDATE orders SET status = 'delivered'  WHERE order_id % 10 < 4;

-- Give the history rows believable timestamps relative to the order date.
UPDATE order_status_history h
   SET changed_at = LEAST(NOW(), o.order_date + CASE h.new_status
                        WHEN 'pending'    THEN INTERVAL '0'
                        WHEN 'processing' THEN INTERVAL '6 hours'
                        WHEN 'shipped'    THEN INTERVAL '1 day'
                        ELSE                   INTERVAL '3 days' END)
  FROM orders o
 WHERE o.order_id = h.order_id;

-- ------------------------------------------------------------------ payments
INSERT INTO payments (order_id, method, amount, status, paid_at)
SELECT o.order_id,
       (ARRAY['card','upi','netbanking','cod'])[1 + (o.order_id % 4)],
       o.total_amount,
       CASE WHEN o.status = 'pending' AND o.order_id % 4 = 3 THEN 'pending'
            ELSE 'completed' END,
       CASE WHEN o.status = 'pending' AND o.order_id % 4 = 3 THEN NULL
            ELSE o.order_date + INTERVAL '5 minutes' END
FROM orders o;

-- Keep statistics fresh for the query planner.
ANALYZE;
