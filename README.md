# CDC Pipeline: Solving Dual-Write & Read/Write Separation

**PostgreSQL → Debezium → Kafka → Consumer → Redis → Read API**, containerized with Docker and deployable on Kubernetes.

The application writes **only** to PostgreSQL (the source of truth). Debezium reads PostgreSQL's
write-ahead log (WAL), publishes every committed row change to Kafka, and a consumer projects those
events into **denormalized documents in Redis**. All customer reads are served from Redis by a
separate Read API that has no database driver at all.

```
                        WRITE PATH                                            READ PATH
 ┌───────────┐  SQL   ┌────────────┐  WAL   ┌──────────┐  events  ┌───────┐        ┌──────────┐  GET  ┌─────────┐   ┌──────────┐
 │ Write API │──────► │ PostgreSQL │──────► │ Debezium │────────► │ Kafka │──────► │ Consumer │─────► │  Redis  │◄──│ Read API │◄── customers
 │ (Flask)   │  ACID  │ 7 tables   │logical │ Connect  │ 7 topics │ ×3    │ group  │projector │ views │read model│   │ (Flask)  │
 └───────────┘  txn   └────────────┘decoding└──────────┘   RF 3   └───────┘        └────┬─────┘       └─────────┘   └──────────┘
      ▲                                                                                 │ metrics                        ▲
      │                                                              Prometheus ◄──────┘ ──► Grafana :3000              │
      └─────────────────────────────── nginx website  http://localhost:8080 ───────────────────────────────────────────┘
```

Slides for Review 2 (ER diagram, schema, architecture, results, fault tolerance, monitoring,
security, work completed): [`docs/review2-slides.pptx`](docs/review2-slides.pptx).

---

## 1. Run it (Windows, Docker Desktop)

```bash
cd cdc-pipeline-project
docker compose up -d --build
```

Open **http://localhost:8080** for the website and **http://localhost:3000** for the Grafana
dashboard. That's it: the connector is registered automatically and the consumer waits for Kafka
and the topics, so no manual restarts are needed.

All configuration (SQL, connector, nginx, Prometheus, Grafana) is built into the images instead
of being mounted from the host, because Docker Desktop's Windows file sharing can fail to mount files.
After editing any of those files, run `docker compose up -d --build` again.

| Container          | What it is                                  | Host port |
|--------------------|---------------------------------------------|-----------|
| `cdc-postgres`     | PostgreSQL 15, `wal_level=logical`, schema + seed + roles | 5433 |
| `cdc-zookeeper`    | Kafka coordination                          | –         |
| `cdc-kafka-1/2/3`  | 3-broker Kafka cluster, replication factor 3, min ISR 2 | 9092–9094 |
| `cdc-connect`      | Debezium Kafka Connect                      | 8083      |
| `cdc-connect-init` | one-shot job that registers the connector, then **exits (normal)** | – |
| `cdc-redis`        | Redis read model                            | 6379      |
| `cdc-consumer`     | Kafka → Redis projector (+ Prometheus metrics) | 8000   |
| `cdc-write-api`    | Write API → PostgreSQL only                 | 5001      |
| `cdc-read-api`     | Read API → Redis only                       | 5002      |
| `cdc-frontend`     | nginx: website + routes `/api/write`, `/api/read` | 8080 |
| `cdc-prometheus`   | scrapes the consumer's metrics every 5 s    | 9090      |
| `cdc-grafana`      | "CDC Pipeline" dashboard, no login needed   | 3000      |

Useful checks:

```bash
docker compose ps
curl http://localhost:8083/connectors/shopdb-connector/status
docker logs -f cdc-consumer
docker exec -it cdc-redis redis-cli GET view:order:1
docker exec -it cdc-postgres psql -U cdcuser -d shopdb
curl http://localhost:8000/metrics
```

Reset everything (deletes the database volume, re-seeds on next start):

```bash
docker compose down -v
```

---

## 2. Demo script for the review (≈ 5 minutes)

The website has three pages:

1. **Shop** (the *Customer* actor): the catalogue comes from Redis. Add two products to the cart,
   pick a customer and place the order. The cart shows the journey
   *PostgreSQL → Debezium → Kafka → Redis* with the measured time from COMMIT until the order
   is readable from Redis (typically 100–200 ms). Stock counts update through CDC too.
2. **Orders** (the *Order Processing System* actor): click **Track this order**. One Redis GET
   returns customer, items, products, categories, payment and status history. Click
   **Mark as processing**: the write goes to PostgreSQL, a trigger writes the history row, and the
   stepper updates once the change arrives through CDC. **Delete order** cascades and restocks.
3. **How it works**: live KPIs and change stream. Click **Generate 20 orders** and watch the events.
   **Run benchmark** compares the data-store time of a 6-table JOIN vs one Redis GET.
   **Run check** proves PostgreSQL and Redis match. **The dual-write problem**: click
   *1. Naive write*, then *2. Naive write + crash*. The naive cache is now permanently stale, while
   the CDC read model is still correct.

Rollbacks and constraint violations (oversell, unknown customer, negative price, invalid status)
are covered by the test suite in section 9; run it live if asked.

---

## 3. Database design (PostgreSQL, 3NF)

| Table                  | Primary key       | Foreign keys                                        | Key attributes / constraints |
|------------------------|-------------------|-----------------------------------------------------|------------------------------|
| `categories`           | `category_id`     | –                                                   | `name` UNIQUE |
| `customers`            | `customer_id`     | –                                                   | `name`, `email` UNIQUE + CHECK, `phone`, `address`, `city`, `created_at` |
| `products`             | `product_id`      | `category_id → categories`                          | `name`, `description`, `price` DECIMAL(10,2) CHECK > 0, `stock` CHECK ≥ 0, `updated_at` |
| `orders`               | `order_id`        | `customer_id → customers`                           | `order_date`, `status` CHECK IN (pending, processing, shipped, delivered), `total_amount` CHECK ≥ 0 |
| `order_items`          | `order_item_id`   | `order_id → orders` (CASCADE), `product_id → products` | `quantity` CHECK > 0, `unit_price`, UNIQUE(order_id, product_id) |
| `payments`             | `payment_id`      | `order_id → orders` (CASCADE)                       | `method` CHECK, `amount`, `status` CHECK, `paid_at` |
| `order_status_history` | `history_id`      | `order_id → orders` (CASCADE)                       | `old_status`, `new_status`, `changed_at` |

Cardinalities: customers 1:N orders · orders 1:N order_items · products 1:N order_items
(so orders M:N products) · categories 1:N products · orders 1:N payments · orders 1:N status history.

**Triggers**
- `trg_order_items_total`: keeps `orders.total_amount = Σ quantity × unit_price` inside the same transaction.
- `trg_orders_status_history`: writes an audit row for every status change.
- `trg_products_touch`: maintains `products.updated_at`.

**Why `unit_price` is not redundant:** it's the price *at purchase time*. A later price change
must not rewrite old orders, so it's a fact about the order line, not a copy of `products.price`.

**CDC settings:** `wal_level=logical`, a publication over the 7 tables, and `REPLICA IDENTITY FULL`
so UPDATE/DELETE events carry the full *before* image (the consumer needs the old status / old
customer to fix its indexes).

Seed data: 6 categories, 24 products, 200 customers, 1,200 orders with ~2,400 items, 1,200 payments
and ~3,500 status-history rows (`postgres-init/02_seed.sql`).

**Database roles (least privilege, `postgres-init/03_roles.sql`)**

| Role         | Used by     | Privileges | Cannot |
|--------------|-------------|------------|--------|
| `cdcuser`    | setup only  | owns the schema | – |
| `app_writer` | Write API   | SELECT, INSERT, UPDATE, DELETE on the 7 tables; sequences | DROP/ALTER tables (`must be owner of table`) |
| `debezium`   | Debezium    | SELECT + `REPLICATION` (to stream the WAL) | modify any data (`permission denied`) |

---

## 4. Write path: one ACID transaction per business operation

`POST /api/write/orders` (see `services/write-api/app.py`):

```sql
BEGIN;
  SELECT ... FROM products WHERE product_id = ANY(...) ORDER BY product_id FOR UPDATE;  -- row locks, fixed order → no deadlocks
  -- check stock (409 + ROLLBACK if insufficient)
  INSERT INTO orders (...) VALUES (..., 'pending', 0) RETURNING order_id;               -- trigger: history row
  INSERT INTO order_items (...);                                                       -- trigger: total_amount
  UPDATE products SET stock = stock - :qty WHERE product_id = :pid;                     -- CHECK (stock >= 0)
  INSERT INTO payments (...);
COMMIT;
```

Other write endpoints: change status, remove an item (`DELETE FROM order_items …`, as on the slides),
delete an order (cascades and restocks atomically), create/update customers, update product price/stock.
Constraint violations (CHECK, UNIQUE, FK) are rejected by PostgreSQL and returned as HTTP 400.

The write API **never** writes the Redis read model. (The only Redis code in it is the clearly
separated `/demo/dual-write` endpoint, which writes to a separate `naive:*` key space to show the problem.)

---

## 5. CDC path: Debezium → Kafka → Consumer

- **Debezium** (`debezium/shopdb-connector.json`): `pgoutput` plugin, replication slot `shopdb_slot`,
  `snapshot.mode=initial` (existing rows are streamed first as `op=r`), decimals as strings (exact money).
- **Kafka**: a 3-broker cluster. One topic per table (`shopdb.public.orders`, …), 3 partitions each,
  every partition replicated to all 3 brokers. Messages are keyed by primary key, so all changes to
  one row are **ordered** within one partition.
- **Consumer** (`consumer/consumer.py`), built on `confluent-kafka` (librdkafka):
  - subscribes by pattern `^shopdb\.public\..*`, consumer group `cdc-redis-projector`, and waits
    for Debezium to create the topics on a fresh start;
  - applies a batch of events to a raw replica + indexes, then rebuilds only the affected
    denormalized views (orders, customers, products);
  - **commits Kafka offsets only after Redis is updated** → at-least-once delivery;
  - every Redis write is an absolute SET/HSET/ZADD (no increments) → **idempotent**, so
    re-processing after a crash is harmless;
  - events can arrive in any order across topics (e.g. an item before its order); views are rebuilt
    from whatever has arrived, so they **converge** once all events are in (eventual consistency);
  - exposes Prometheus metrics on `:8000/metrics`: `cdc_events_total`, `cdc_end_to_end_lag_seconds`,
    `cdc_kafka_consumer_lag`, `cdc_redis_keyspace_hits/misses`, `cdc_view_rebuilds_total`.

### Fault tolerance

| Setting | Value | Effect |
|---|---|---|
| Brokers | 3 | the cluster keeps working with one broker down |
| `default.replication.factor` | 3 | every event is stored on all three brokers |
| `min.insync.replicas` + producer `acks` | 2 + `all` | a write counts only once at least 2 brokers have it |
| `unclean.leader.election.enable` | false | an out-of-date replica can never become leader (no silent data loss) |
| Consumer offsets | committed after Redis is updated | at-least-once; replays are idempotent |

`tests/test_failover.py` proves it live: it stops broker 2, keeps placing orders, checks every one
reaches Redis, restarts the broker and waits for full replication. Result: **30/30 orders survived,
zero lost**, fully re-replicated 15 s after the restart. While the broker is down, new changes reach
Redis in 1–16 s (longer when the stopped broker was coordinating the consumer group).

### Monitoring

Prometheus scrapes the consumer every 5 s; Grafana (http://localhost:3000, no login) opens on the
**CDC Pipeline** dashboard: change events per second by table, commit → Redis latency p50/p95/p99,
Kafka consumer lag by table, Redis hit rate, view rebuilds and read-model size. Under simulated load:
p50 ≈ 90 ms, p95 ≈ 210 ms, Redis hit rate ≈ 99%, lag back to 0 within seconds.

### Redis read model

| Key                          | Type   | Contents |
|------------------------------|--------|----------|
| `view:order:<id>`            | string | order + customer + items (product, category, qty, price) + payment + history |
| `view:customer:<id>`         | string | customer + `order_count`, `total_spent`, `last_order_date` |
| `view:product:<id>`          | string | product + category name + live stock |
| `orders:recent`              | zset   | order ids by order date |
| `orders:status:<status>`     | zset   | order ids per status |
| `orders:totals`              | hash   | order id → total (revenue KPI) |
| `customers:all`, `products:all` | zset | listing indexes |
| `raw:<table>:<pk>`           | string | 1:1 replica of each row (used to build views) |
| `idx:*`                      | set    | reverse indexes (customer → orders, order → items, …) |
| `cdc:events`, `cdc:stats`    | list / hash | pipeline telemetry shown on the dashboard |

---

## 6. Read path

`services/read-api/app.py` imports only `redis`. Example: `GET /api/read/orders/42` is a single
`GET view:order:42`. The equivalent Postgres query joins 6 tables
(`GET /api/write/direct/orders/42`, kept only for the benchmark and consistency check).

---

## 7. DBMS concept mapping

| Concept | Where it is in the project |
|---|---|
| Normalization (3NF) | 7-table schema; M:N via `order_items`; no transitive dependencies |
| Integrity constraints | PK, FK + NOT NULL, UNIQUE, CHECK (status, money ≥ 0, stock ≥ 0, email) |
| ACID / transactions | order placement, order deletion with restock, COD settlement on delivery |
| Concurrency control | `SELECT … FOR UPDATE` row locks in a fixed order (deadlock avoidance) |
| Triggers | total recomputation, status audit log, `updated_at` |
| Durability / WAL | Debezium reads the WAL via logical decoding; Kafka persists events; Redis AOF |
| Replication vs denormalization | raw replica + pre-joined views in Redis |
| Eventual consistency | Redis converges to PostgreSQL; consistency checker proves it |
| Indexing | B-tree indexes on all FK columns and `orders.status`; sorted sets in Redis |
| Security / privileges | `GRANT`-based least-privilege roles for the app and for CDC; K8s NetworkPolicies |

---

## 8. Kubernetes

**Easiest: a real cluster (k3s) inside Docker, nothing else to install.** Verified on this project:
12 pods running, 15/15 end-to-end tests passing against the cluster, NetworkPolicies enforced
(read-api → Postgres and frontend → Redis are refused), and the HPA scaled the consumer 2 → 3.

```bash
powershell -ExecutionPolicy Bypass -File k8s/k3s-local.ps1
```

It stops the Compose stack (to free memory), builds the images, starts k3s, copies the images in,
applies the manifests and waits for the pods. Website at http://localhost:30080. Afterwards:

```bash
powershell -ExecutionPolicy Bypass -File k8s/k3s-local.ps1 -Delete
```

**With Docker Desktop Kubernetes or Minikube** instead:

```bash
powershell -ExecutionPolicy Bypass -File k8s/deploy.ps1
kubectl -n cdc get pods -w
```

Website at http://localhost:30080 (Minikube: `minikube -n cdc service frontend --url`). The
Kubernetes manifests use a single Kafka broker to fit a laptop; the 3-broker setup is in Docker Compose.

- `k8s/00-namespace-secrets.yaml`: namespace `cdc`, PostgreSQL credentials as a Secret
- `k8s/10-infrastructure.yaml`: PostgreSQL StatefulSet with a PVC, ZooKeeper, Kafka, Debezium Connect, connector-registration Job, Redis
- `k8s/20-applications.yaml`: consumer (2 replicas), write-api, read-api (2 replicas), frontend NodePort, HPAs for read-api and consumer
- `k8s/30-network-policies.yaml`: only write-api and Debezium may reach PostgreSQL; only consumer/read-api/write-api may reach Redis

Scale the consumer and watch Kafka rebalance partitions between replicas:

```bash
kubectl -n cdc scale deployment consumer --replicas=3
kubectl -n cdc logs -l app=consumer -f
```

---

## 9. Benchmark from the command line

```bash
pip install -r benchmark/requirements.txt
python benchmark/benchmark.py --reads 4000 --clients 4
python benchmark/benchmark.py --reads 4000 --clients 4 --writers 4 --csv under_load.csv
```

No local Python? Run it in a throwaway container on the stack's network:

```bash
docker run --rm --network cdc-pipeline-project_default -v "%cd%/benchmark:/b" python:3.11-slim sh -c "pip install -q -r /b/requirements.txt && python /b/benchmark.py --pg-host postgres --pg-port 5432 --redis-host redis --reads 4000 --clients 4 --writers 4"
```

Measured on the development machine (1,231 orders, 4 reader processes, full order document):

| Scenario | PostgreSQL 6-table JOIN | Redis pre-joined view | Redis advantage |
|---|---|---|---|
| Reads only | 1.24 ms avg · 3,167 reads/s | 0.16 ms avg · 24,224 reads/s | 7.9× lower latency, 7.6× throughput |
| Reads + 4 concurrent writers | 2.20 ms avg · 1,770 reads/s | 0.45 ms avg · 8,550 reads/s | 4.8× lower latency, 4.8× throughput |

Key observation: with concurrent writes, Postgres read latency almost **doubles** (1.24 → 2.20 ms).
That is the heavy-read/write contention from the problem statement, and the reads served from Redis
take that load off the OLTP database entirely.

End-to-end CDC latency (Postgres COMMIT → visible through the Read API): ~100 ms median
(76–193 ms over 10 status updates).

The website's *Read speed* panel shows the time spent in the data store, which the APIs report in a
`Server-Timing` header (about 4× in the browser, where the Flask workers share CPU). It also shows
the full HTTP request time, where both paths carry the same ~15 ms web-server overhead.
The command-line benchmark uses separate reader processes because Python threads would contend on
the GIL inside the benchmark client and hide the difference.

---

## 10. Automated end-to-end tests

```bash
python tests/test_e2e.py
```

Standard library only (no `pip install`). 15 tests write through the Write API and wait for the change
to arrive through the Read API: order creation, stock, status history trigger, COD settlement,
item removal and total recomputation, cascade delete and restock, customer change propagating into
denormalized orders, oversell rollback, FK/CHECK/status rejections, the dual-write failure, full
PostgreSQL vs Redis convergence, and commit → Redis latency under one second.
Last run: **15/15 passed** on Docker Compose and on Kubernetes (k3s).

Broker-failure test (needs the `docker` CLI; stops and restarts `cdc-kafka-2`):

```bash
python tests/test_failover.py
```

---

## 11. Troubleshooting

| Symptom | Fix |
|---|---|
| Page loads but nothing works / red "Can't reach the backend" banner | The Docker stack isn't running. Start it with `docker compose up -d --build`, then use http://localhost:8080 in a normal browser. The page retries automatically and recovers by itself once the stack is up. |
| "Waiting for the first change events…" | Connect takes 30–60 s on first start. `docker logs cdc-connect-init` shows the registration result. |
| `cdc-connect-init` shows *Exited (0)* | Normal: it's a one-shot job. |
| Port 5433/6379/8080 already in use | Stop the other program, or change the left side of the port mapping in `docker-compose.yml`. |
| Changed `postgres-init/*.sql` but nothing changed | Init scripts only run on an empty volume: `docker compose down -v` then `up -d --build`. |
| Changed the website, nginx, Prometheus or Grafana config but nothing changed | Config is built into the images: run `docker compose up -d --build`. |
| Grafana shows "No data" | Generate some traffic (website → How it works → Generate 20 orders); panels fill within ~10 s. |
| Want to re-register the connector | `docker compose run --rm connect-init` |
