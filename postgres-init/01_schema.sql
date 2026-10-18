-- ============================================================================
--  shopdb  --  WRITE-SIDE SOURCE OF TRUTH (normalized to 3NF)
-- ============================================================================
--  Entities (7 tables):
--
--    categories 1 ─── N products
--    customers  1 ─── N orders
--    orders     1 ─── N order_items  N ─── 1 products     (orders M:N products)
--    orders     1 ─── N payments
--    orders     1 ─── N order_status_history               (audit trail, trigger)
--
--  Every FK column is NOT NULL + FOREIGN KEY, every money column is
--  DECIMAL(10,2) with a CHECK, and order status is constrained by CHECK.
--
--  The application ONLY writes here. Redis is never written by the app;
--  it is derived from this database's WAL by Debezium -> Kafka -> consumer.
-- ============================================================================

-- ---------------------------------------------------------------- categories
CREATE TABLE categories (
    category_id   SERIAL       PRIMARY KEY,
    name          VARCHAR(60)  NOT NULL UNIQUE,
    description   TEXT
);

-- ----------------------------------------------------------------- customers
CREATE TABLE customers (
    customer_id   SERIAL        PRIMARY KEY,
    name          VARCHAR(120)  NOT NULL,
    email         VARCHAR(160)  NOT NULL UNIQUE
                  CHECK (email LIKE '%_@_%._%'),
    phone         VARCHAR(20),
    address       TEXT          NOT NULL,
    city          VARCHAR(60)   NOT NULL,
    created_at    TIMESTAMP     NOT NULL DEFAULT NOW()
);

-- ------------------------------------------------------------------ products
CREATE TABLE products (
    product_id    SERIAL         PRIMARY KEY,
    category_id   INT            NOT NULL REFERENCES categories(category_id),
    name          VARCHAR(160)   NOT NULL,
    description   TEXT,
    price         DECIMAL(10,2)  NOT NULL CHECK (price > 0),
    stock         INT            NOT NULL DEFAULT 0 CHECK (stock >= 0),
    updated_at    TIMESTAMP      NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------------- orders
-- Same DDL as on the "Sample Database Operations" slide.
CREATE TABLE orders (
    order_id      SERIAL PRIMARY KEY,
    customer_id   INT NOT NULL REFERENCES customers(customer_id),
    order_date    TIMESTAMP NOT NULL DEFAULT NOW(),
    status        VARCHAR(20) NOT NULL
                  CHECK (status IN ('pending','processing','shipped','delivered')),
    total_amount  DECIMAL(10,2) NOT NULL CHECK (total_amount >= 0)
);

-- --------------------------------------------------------------- order_items
-- Resolves the M:N between orders and products. unit_price is a snapshot of
-- the product price at purchase time (a later price change must not alter
-- historical orders), so it is NOT a transitive dependency on products.
CREATE TABLE order_items (
    order_item_id SERIAL         PRIMARY KEY,
    order_id      INT            NOT NULL REFERENCES orders(order_id)   ON DELETE CASCADE,
    product_id    INT            NOT NULL REFERENCES products(product_id),
    quantity      INT            NOT NULL CHECK (quantity > 0),
    unit_price    DECIMAL(10,2)  NOT NULL CHECK (unit_price >= 0),
    UNIQUE (order_id, product_id)
);

-- ------------------------------------------------------------------ payments
CREATE TABLE payments (
    payment_id    SERIAL         PRIMARY KEY,
    order_id      INT            NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    method        VARCHAR(20)    NOT NULL
                  CHECK (method IN ('card','upi','netbanking','cod')),
    amount        DECIMAL(10,2)  NOT NULL CHECK (amount >= 0),
    status        VARCHAR(20)    NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending','completed','failed','refunded')),
    paid_at       TIMESTAMP
);

-- ------------------------------------------------------ order_status_history
-- Filled automatically by a trigger; never written by the application.
CREATE TABLE order_status_history (
    history_id    SERIAL       PRIMARY KEY,
    order_id      INT          NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    old_status    VARCHAR(20),
    new_status    VARCHAR(20)  NOT NULL,
    changed_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- ------------------------------------------------------------------- indexes
CREATE INDEX idx_products_category   ON products(category_id);
CREATE INDEX idx_orders_customer     ON orders(customer_id);
CREATE INDEX idx_orders_status       ON orders(status);
CREATE INDEX idx_order_items_order   ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_payments_order      ON payments(order_id);
CREATE INDEX idx_history_order       ON order_status_history(order_id);

-- ================================================================== triggers

-- 1. Keep orders.total_amount = SUM(quantity * unit_price) of its items.
--    Runs inside the same transaction as the item change, so it is atomic.
CREATE FUNCTION fn_recalc_order_total() RETURNS TRIGGER AS $$
DECLARE
    oid INT := COALESCE(NEW.order_id, OLD.order_id);
BEGIN
    UPDATE orders
       SET total_amount = COALESCE(
               (SELECT SUM(quantity * unit_price) FROM order_items WHERE order_id = oid), 0)
     WHERE order_id = oid;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_order_items_total
AFTER INSERT OR UPDATE OR DELETE ON order_items
FOR EACH ROW EXECUTE FUNCTION fn_recalc_order_total();

-- 2. Audit every status transition into order_status_history.
CREATE FUNCTION fn_log_status_change() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO order_status_history(order_id, old_status, new_status)
        VALUES (NEW.order_id, NULL, NEW.status);
    ELSIF NEW.status IS DISTINCT FROM OLD.status THEN
        INSERT INTO order_status_history(order_id, old_status, new_status)
        VALUES (NEW.order_id, OLD.status, NEW.status);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orders_status_history
AFTER INSERT OR UPDATE OF status ON orders
FOR EACH ROW EXECUTE FUNCTION fn_log_status_change();

-- 3. Touch products.updated_at on every change.
CREATE FUNCTION fn_touch_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_products_touch
BEFORE UPDATE ON products
FOR EACH ROW EXECUTE FUNCTION fn_touch_updated_at();

-- ======================================================================= CDC
-- REPLICA IDENTITY FULL makes UPDATE/DELETE events carry the complete
-- "before" row image (not just the PK). The consumer needs the old status
-- and the old customer_id/product_id to fix up its Redis indexes.
ALTER TABLE categories           REPLICA IDENTITY FULL;
ALTER TABLE customers            REPLICA IDENTITY FULL;
ALTER TABLE products             REPLICA IDENTITY FULL;
ALTER TABLE orders               REPLICA IDENTITY FULL;
ALTER TABLE order_items          REPLICA IDENTITY FULL;
ALTER TABLE payments             REPLICA IDENTITY FULL;
ALTER TABLE order_status_history REPLICA IDENTITY FULL;

-- Publication used by Debezium's pgoutput plugin.
CREATE PUBLICATION shopdb_publication FOR TABLE
    categories, customers, products, orders,
    order_items, payments, order_status_history;
