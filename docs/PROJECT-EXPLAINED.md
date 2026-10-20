# CDC Pipeline: The Whole Project, Explained Simply

This document explains, end to end and in simple terms:

1. [The big idea (why this project exists)](#1-the-big-idea)
2. [Containerization and Docker](#2-containerization-and-docker)
3. [The Azure deployment](#3-the-azure-deployment)
4. [ShopDB, end to end (database → Debezium → Kafka → Redis → website)](#4-shopdb-end-to-end)
5. [One order, followed through every step](#5-one-order-followed-through-every-step)
6. [Grafana and every element on the dashboard](#6-grafana-and-the-dashboard)
7. [How to start, demo and stop it](#7-how-to-start-demo-and-stop)
8. [Questions a professor may ask](#8-questions-a-professor-may-ask)
9. [Glossary](#9-glossary)

---

## 1. The big idea

### The problem: "dual write"
An online shop keeps its data in a **database** (PostgreSQL). To make pages load fast, it also
keeps a copy in a **cache** (Redis), which is a very fast in-memory store.

The usual approach: the app writes the order to the database **and then** writes it to the cache.
This is a *dual write*. If the app crashes **between** the two writes, the database has the new
data but the cache still has the old data, and nobody ever fixes it. Customers see wrong
information forever.

### Our solution: Change Data Capture (CDC)
- The app writes **only** to PostgreSQL, the single *source of truth*.
- PostgreSQL already records every change in its own log (the **WAL**, write-ahead log).
- **Debezium** reads that log and publishes every change as a message to **Kafka**.
- A **consumer** program reads the messages from Kafka and updates **Redis**.
- Customers read from Redis through a separate **Read API**.

Since Redis is rebuilt from the database's own log, it can't permanently miss a change, even if
the app crashes. At worst it is a few hundred milliseconds behind. That small delay is called
**eventual consistency**.

```
WRITE PATH :  Browser → Write API → PostgreSQL                       (only writer of the DB)
CDC PATH   :  PostgreSQL WAL → Debezium → Kafka (3 brokers) → Consumer → Redis
READ PATH  :  Browser → Read API → Redis                             (never touches the DB)
MONITORING :  Consumer metrics → Prometheus → Grafana
```

Keeping the write side and the read side separate is called **read/write separation**, or **CQRS**
(Command Query Responsibility Segregation).

---

## 2. Containerization and Docker

### What is containerization?
A **container** is a sealed box holding an application **plus everything it needs to run**: the
code, the language runtime (e.g. Python 3.11), libraries and configuration. The box runs the same
way on any computer that has Docker: your laptop, a classmate's laptop or a cloud server.

Analogy: a shipping container. A port doesn't care whether it holds bananas or laptops, because
every container has the same shape and handling. Docker containers are the same: a server doesn't
need to know whether a container runs Python, Java or PostgreSQL. It just runs it.

**Container vs virtual machine:**

| | Virtual machine (VM) | Container |
|---|---|---|
| Contains | A whole operating system + app | Just the app + its libraries |
| Size | Gigabytes | Megabytes to a few hundred MB |
| Starts in | Minutes | Seconds |
| Isolation | Very strong (separate OS) | Strong (shares the host's Linux kernel) |

Our project uses **both**: one Azure **VM**, running **14 containers** inside it.

### Why containerize this project?
- **"Works on my machine" problem solved:** Kafka, Debezium, PostgreSQL, Redis, Python and nginx all
  come pre-packaged. Nobody installs anything by hand.
- **One command starts everything:** `docker compose up -d`.
- **Isolation:** each component runs in its own container and can be restarted, replaced or scaled
  without affecting the others.
- **Portability:** the same images ran on the laptop and then on Azure, unchanged.

### Docker vocabulary

| Term | Simple meaning | In this project |
|---|---|---|
| **Dockerfile** | A recipe for building an image | e.g. `services/write-api/Dockerfile`: start from Python 3.11, install Flask, copy `app.py`, run it with gunicorn |
| **Image** | The built, read-only package (the "frozen box") | `cdc-write-api:latest`, `cdc-postgres:latest`, … |
| **Container** | A running instance of an image | `cdc-write-api`, `cdc-kafka-1`, … |
| **Registry** | A storage server for images | **Azure Container Registry** (`cdcpipelined3a62399.azurecr.io`) |
| **Docker Compose** | A tool that starts many containers together from one YAML file | `docker-compose.yml` (laptop) / `azure/docker-compose.azure.yml` (Azure) |
| **Network** | A private network between containers | Containers find each other by name, e.g. the consumer connects to `redis:6379` |
| **Volume** | Storage that survives container restarts | `pgdata`, which holds the PostgreSQL data files |
| **Port mapping** | Exposing a container's port on the host | `8080:80` means host port 8080 → nginx inside the container on port 80 |
| **Healthcheck** | A command Docker runs to see if a container is healthy | e.g. `pg_isready` for PostgreSQL, `redis-cli ping` for Redis |
| **Restart policy** | What Docker does if a container stops | `unless-stopped`: restart automatically, including after a reboot |
| **depends_on** | Startup order | Debezium waits until all 3 Kafka brokers and PostgreSQL are *healthy* |

### Our 14 containers

| Container | Image | Built by us? | Job |
|---|---|---|---|
| `cdc-postgres` | `cdc-postgres` (from `postgres:15`) | ✅ schema, seed data and roles baked in | The database (source of truth) |
| `cdc-zookeeper` | `confluentinc/cp-zookeeper:7.6.0` | ❌ official | Coordinates the Kafka brokers |
| `cdc-kafka-1/2/3` | `confluentinc/cp-kafka:7.6.0` | ❌ official | 3-broker Kafka cluster (the event log) |
| `cdc-connect` | `debezium/connect:2.6` | ❌ official | Debezium: reads the WAL, writes to Kafka |
| `cdc-connect-init` | `cdc-connect-init` | ✅ | One-time job: registers the Debezium connector, then **exits with code 0 (success)** |
| `cdc-redis` | `redis:7-alpine` | ❌ official | The fast read model |
| `cdc-consumer` | `cdc-consumer` (Python) | ✅ | Reads Kafka, writes Redis, exposes metrics |
| `cdc-write-api` | `cdc-write-api` (Python/Flask) | ✅ | Handles all writes → PostgreSQL only |
| `cdc-read-api` | `cdc-read-api` (Python/Flask) | ✅ | Handles all reads → Redis only |
| `cdc-frontend` | `cdc-frontend` (nginx) | ✅ | Serves the website and routes `/api/write` and `/api/read` |
| `cdc-prometheus` | `cdc-prometheus` | ✅ config baked in | Collects metrics every 5 s |
| `cdc-grafana` | `cdc-grafana` | ✅ dashboard baked in | Draws the monitoring dashboard |

"Baked in" means the configuration files are copied **into the image** at build time instead of
being mounted from the host. That makes each image self-contained, which is what lets it run on
Azure unchanged.

---

## 3. The Azure deployment

### What we built on Azure

```
                    ┌─────────────────────────── Microsoft Azure ─────────────────────────────┐
                    │  Resource group: cdc-pipeline-rg                                         │
 Laptop             │                                                                          │
 (Docker build) ──push──►  Azure Container Registry  (Central India)                          │
                    │      cdcpipelined3a62399.azurecr.io   8 private images                   │
                    │                │ pull                                                    │
                    │                ▼                                                         │
 Professor's  ──────┼──► Public IP 20.214.184.171 ─► Network Security Group (firewall)         │
 browser            │                                  allows only 8080, 3000, 22              │
                    │                                        │                                 │
                    │                ┌───────────────────────▼──────────────────────────┐      │
                    │                │ VM: cdc-pipeline-vm (Korea Central)              │      │
                    │                │ Ubuntu 22.04 · Standard_B2as_v2 · 2 vCPU · 8 GB  │      │
                    │                │ Docker Engine + Docker Compose                   │      │
                    │                │   └── 14 containers (Section 2)                  │      │
                    │                └──────────────────────────────────────────────────┘      │
                    └──────────────────────────────────────────────────────────────────────────┘
```

### Each Azure resource, explained

| Resource | Name | What it is | Why we need it |
|---|---|---|---|
| **Subscription** | Azure for Students | The billing account ($100 student credit) | Pays for everything |
| **Resource group** | `cdc-pipeline-rg` | A folder that groups related resources | Manage, view or delete the whole project in one place |
| **Container registry (ACR)** | `cdcpipelined3a62399` | A private Docker image store, like a private Docker Hub | The VM pulls our 8 custom images from here |
| **Virtual machine** | `cdc-pipeline-vm` | A Linux computer rented in Azure's datacenter | Runs Docker and the 14 containers |
| **Public IP** | `20.214.184.171` | The VM's internet address (static: it doesn't change on stop/start) | So anyone can open the website and Grafana |
| **Network security group (NSG)** | `cdc-pipeline-vmNSG` | A firewall with allow rules | Only ports **8080** (website), **3000** (Grafana) and **22** (SSH, admin) are open |
| **Virtual network + NIC** | auto-created | The VM's private network and network card | Required for any VM |
| **OS disk** | 64 GB | The VM's hard disk | Stores Docker images and the database volume |

### How the deployment was done, step by step
1. **Sign in to Azure** with the Azure CLI (`az login`, approved in the browser).
2. **Create the resource group** `cdc-pipeline-rg`.
3. **Create the container registry** (Basic tier).
4. **Build the images locally with Docker** (`docker compose build`) from the project's Dockerfiles.
5. **Tag and push** the 8 custom images to ACR (`docker push cdcpipelined3a62399.azurecr.io/cdc-…`).
6. **Create the VM** with a **cloud-init** script (`azure/cloud-init.yaml`). cloud-init runs automatically
   the first time the VM boots, and here it installs Docker.
7. **Open ports** 8080 and 3000 in the firewall.
8. **Copy** `azure/docker-compose.azure.yml` to the VM (`/opt/cdc/`). It's the same as the laptop
   compose file, except each image comes from ACR instead of being built locally.
9. On the VM: **log in to ACR, pull the images, and run `docker compose up -d`**.
10. **Verify:** website (200 OK), Grafana (200 OK), Debezium connector `RUNNING`, and Redis
    contains the same data as PostgreSQL.

All of this is automated in [`azure/deploy-azure.sh`](../azure/deploy-azure.sh).

### Why a single VM with Docker Compose (and not Kubernetes or Azure Container Apps)?
- The 3 Kafka brokers, ZooKeeper and Debezium must all reach each other on **one shared private
  network** with stable names. Docker Compose provides that in one simple file.
- **Cost and quotas:** Azure for Students limits CPU cores and regions. Kubernetes (AKS) would need
  several VMs and more credit.
- **Upgrade path:** the repo already has Kubernetes manifests in [`k8s/`](../k8s/) to scale out later.

### Why is the VM in Korea Central?
Azure for Students only allows certain regions (Central India, Korea Central, East Asia, Austria
East, Malaysia West). When we deployed, Central India had **no VM capacity** for this subscription,
so the VM went to Korea Central. The registry stayed in Central India, and the VM pulls from it
across regions.

### Security on Azure
- Only 3 ports are open. PostgreSQL (5432), Kafka (9092–9094) and Redis (6379) **cannot** be
  reached from the internet; containers talk to them over Docker's private network.
- The image registry is **private** (login required).
- Grafana: anyone can *view* the dashboard (read-only). *Editing* needs the admin password, which
  was changed from the default `admin`.
- Database roles follow **least privilege** (see Section 4.2).

---

## 4. ShopDB end to end

ShopDB is the shop's PostgreSQL database. Data flows through these stages:

```
 ① Write API ─► ② PostgreSQL (shopdb) ─► ③ WAL ─► ④ Debezium ─► ⑤ Kafka ─► ⑥ Consumer ─► ⑦ Redis ─► ⑧ Read API ─► ⑨ Website
```

### 4.1 The tables (3rd normal form)

| Table | What one row is | Key columns and rules |
|---|---|---|
| `categories` | A product category (Electronics, Books, …) | `name` is UNIQUE |
| `customers` | A shopper | `email` UNIQUE + must look like an email (CHECK) |
| `products` | An item for sale | `price > 0`, `stock ≥ 0` (CHECK), belongs to a category (foreign key) |
| `orders` | One purchase by a customer | `status` must be pending / processing / shipped / delivered; `total_amount ≥ 0` |
| `order_items` | One line in an order ("2 × Earbuds") | `quantity > 0`; `unit_price` is the price **at purchase time**; UNIQUE(order, product) |
| `payments` | Payment for an order | `method` ∈ card / upi / netbanking / cod |
| `order_status_history` | An audit log of status changes | Filled **only by a trigger**, never by the app |

**Relationships:** a customer has many orders; an order has many items; a product appears in many
order items (so **orders ↔ products is many-to-many** through `order_items`); a category has many
products; an order has payments and a status history.
Deleting an order **cascades** to its items, payments and history.

**Seed data:** 6 categories, 24 products, 200 customers, 1,200 orders, ~2,400 items,
1,200 payments, ~3,400 history rows.

**Why `unit_price` isn't redundant:** if the Smartwatch price changes from ₹4,999 to ₹5,499 next
month, old orders must still show ₹4,999. So the price at purchase time is a fact about the order
line, not a copy of `products.price`.

### 4.2 Rules the database enforces itself

**Triggers (automatic actions inside the database):**
1. `trg_order_items_total`: whenever an item is added, changed or removed, it recalculates
   `orders.total_amount = Σ quantity × unit_price`, inside the **same transaction**.
2. `trg_orders_status_history`: whenever an order is created or its status changes, it writes a row to
   `order_status_history`.
3. `trg_products_touch`: updates `products.updated_at` on every product change.

**Indexes** on all foreign-key columns (e.g. `orders(customer_id)`, `orders(status)`) make lookups fast.

**Roles (least privilege):**

| Role | Used by | Can | Cannot |
|---|---|---|---|
| `cdcuser` | Setup only | Owns the schema | – |
| `app_writer` | Write API | SELECT / INSERT / UPDATE / DELETE rows | DROP or ALTER tables |
| `debezium` | Debezium | SELECT + **REPLICATION** (read the WAL) | Modify any data |

### 4.3 ① Write API: one ACID transaction per action
When a customer places an order, the Write API runs **one transaction**:
1. Check that the customer exists.
2. **Lock the product rows** (`SELECT … FOR UPDATE`, always in product-id order so two orders can
   never deadlock), then check stock. This is **concurrency control**: two people can't both buy the
   last Smartwatch.
3. `INSERT` the order (status `pending`). The trigger adds a history row.
4. For each product: `INSERT` an order item (the trigger updates the order total) and reduce stock.
5. `INSERT` the payment.
6. **COMMIT**: everything is saved together, or (on any error) **nothing** is saved (rollback).

ACID = **A**tomic (all or nothing), **C**onsistent (all rules hold), **I**solated (concurrent
orders don't interfere), **D**urable (once committed, it survives a crash).

Errors are enforced by the database itself: overselling stock, an unknown customer, a negative
price or an invalid status are all rejected with a constraint violation.

### 4.4 ② → ③ PostgreSQL and the WAL
Before PostgreSQL changes any data file, it first appends the change to the **write-ahead log (WAL)**.
That's how it recovers after a crash. We start PostgreSQL with `wal_level=logical`, so the WAL can be
decoded into readable row changes ("row 1201 inserted into orders with these values").

Two more settings make this work for CDC:
- **Publication** `shopdb_publication`: the list of the 7 tables whose changes are published.
- **`REPLICA IDENTITY FULL`**: UPDATE and DELETE events carry the **whole old row** ("before") as well
  as the new row ("after"), so the consumer knows, for example, that an order moved *from* `pending`
  *to* `processing`.

### 4.5 ④ Debezium (Kafka Connect)
Debezium connects to PostgreSQL as the `debezium` user and opens a **replication slot**
(`shopdb_slot`), a bookmark in the WAL so no change is ever skipped, even if Debezium restarts.

- **Initial snapshot:** on first start, it reads every existing row (the seed data) and sends each
  one as an event with `op = "r"` (read).
- **Streaming:** after that, every committed change becomes an event:
  `op = "c"` (create/insert), `"u"` (update) or `"d"` (delete).

Each event looks like:
```json
{ "op": "u",
  "before": { "order_id": 1201, "status": "pending",    ... },
  "after":  { "order_id": 1201, "status": "processing", ... },
  "source": { "table": "orders", "ts_ms": 1792483187677 } }
```
`ts_ms` is the time of the database commit. The consumer uses it to measure latency.

The `cdc-connect-init` container simply sends Debezium's configuration (`debezium/shopdb-connector.json`)
to Kafka Connect's REST API, then exits. That's why it shows **Exited (0)**, which is normal.

### 4.6 ⑤ Kafka: the event log
Kafka stores events in **topics**, one per table: `shopdb.public.orders`, `shopdb.public.order_items`,
`shopdb.public.products`, and so on (7 topics).

- **3 brokers** (`kafka-1/2/3`): every topic is **replicated 3 times** (replication factor 3).
- **`min.insync.replicas = 2`** and **`acks = all`**: an event counts as saved only when at least
  **2 brokers** have it. So **any one broker can die** without losing an event or stopping the pipeline.
- **3 partitions per topic:** up to 3 consumer copies could share the work.
- Events are **keyed by primary key**, so all changes to one row land in the same partition and are
  read **in commit order**.
- **ZooKeeper** keeps track of which brokers are alive and which one leads each partition.

### 4.7 ⑥ The consumer (projector)
A Python program, the **only writer of Redis**. For each batch of events it:
1. Saves a **raw copy** of the row: `raw:orders:1201`.
2. Updates **indexes**, e.g. `idx:customer:149:orders` = the set of order IDs for customer 149.
3. **Rebuilds the affected "views"**: ready-made JSON documents for the website
   (`view:order:1201`, `view:customer:149`, `view:product:2`).
4. Only **after** Redis is updated does it tell Kafka "I've processed up to here" (commits the **offset**).

This gives **at-least-once delivery**: if the consumer crashes, it re-reads some events. Every Redis
write *sets* a value (it never adds to one), so processing an event twice gives the same result. This
is called **idempotent**, and it means no duplicates and no drift.

It also publishes **metrics** on port 8000 (`/metrics`) for Prometheus (Section 6).

### 4.8 ⑦ Redis: the read model

| Key pattern | What it holds |
|---|---|
| `raw:<table>:<id>` | A 1:1 copy of each PostgreSQL row |
| `idx:customer:<id>:orders`, `idx:order:<id>:items`, … | Reverse indexes (which orders belong to whom) |
| `view:order:<id>` | **Denormalized** order: customer + items + product names + categories + payment + full status history |
| `view:customer:<id>`, `view:product:<id>` | Ready-made customer and product pages |
| `orders:recent`, `orders:status:<status>` | Sorted lists for the Orders page and filters |
| `cdc:events`, `cdc:stats` | The live change stream and counters shown on the website |

About **13,650 keys** after the initial load.

**Denormalized** means the data is stored *pre-joined*. In PostgreSQL, showing one order needs a
**JOIN across 6 tables**; in Redis it's **one GET** of `view:order:<id>`.

### 4.9 ⑧ Read API and ⑨ website
- **Read API** (`/api/read/...`): Flask service that reads **only Redis**. It doesn't even have a
  PostgreSQL driver installed, so reads can never load the database.
- **nginx frontend** (port 8080): serves the website (`index.html`) and routes
  `/api/write/*` → Write API and `/api/read/*` → Read API.
- **Website pages:** **Shop** (customer places orders), **Orders** (track and advance order status),
  **How it works** (live KPIs, change stream, *Generate 20 orders*, benchmark, consistency check,
  dual-write demo).

---

## 5. One order, followed through every step

**Example:** Lakshmi Rao (customer 149) buys **1 × Wireless Earbuds (₹2,499)** and **1 × Smartwatch (₹4,999)**, paying by UPI.

| # | Where | What happens |
|---|---|---|
| 1 | Website | Click **Place order** → `POST /api/write/orders` |
| 2 | nginx | Routes `/api/write/…` to the Write API container |
| 3 | Write API | Starts a transaction, locks the 2 product rows, checks stock |
| 4 | PostgreSQL | `INSERT` order 1201 (pending, total 0) → **trigger** inserts a history row (NULL → pending) |
| 5 | PostgreSQL | `INSERT` item Earbuds → **trigger** sets total to ₹2,499; stock of Earbuds −1 |
| 6 | PostgreSQL | `INSERT` item Smartwatch → **trigger** sets total to ₹7,498; stock of Smartwatch −1 |
| 7 | PostgreSQL | `INSERT` payment ₹7,498, UPI, completed |
| 8 | PostgreSQL | **COMMIT**: all of it is written to the WAL at once |
| 9 | Debezium | Reads the WAL and emits **9 events**: orders ×3 (1 insert + 2 total updates), history ×1, order_items ×2, products ×2, payments ×1 |
| 10 | Kafka | Each event is stored on 3 brokers, in topics `shopdb.public.orders`, `…order_items`, etc. |
| 11 | Consumer | Applies the 9 events, rebuilds `view:order:1201`, `view:customer:149`, `view:product:<earbuds>`, `view:product:<smartwatch>` |
| 12 | Redis | New and updated keys are ready |
| 13 | Website | Polls the Read API until order 1201 appears, then shows *"readable from Redis in ~150 ms"* |
| 14 | Grafana | Within 5 s: events/s goes up, latency is recorded, keys go up, lag returns to 0 |

That's why **Generate 20 orders** produces about **20 × 9 ≈ 180 events**, and why every coloured
line in Grafana moves at the same moment.

---

## 6. Grafana and the dashboard

### What Grafana and Prometheus are
- **Metrics:** numbers a program reports about itself, e.g. "events processed so far = 8,390".
- **Prometheus:** a database for numbers over time (a *time-series database*). Every **5 seconds** it
  visits the consumer's `/metrics` page and stores the values with a timestamp.
- **Grafana:** a dashboard tool. It asks Prometheus questions (in a query language called **PromQL**)
  and draws the answers as numbers and graphs. It stores no data itself.

```
Consumer (:8000/metrics) ──every 5 s──► Prometheus (:9090) ──PromQL queries──► Grafana (:3000) ──► your browser
```

Grafana watches the **pipeline's behaviour** (speed, backlog, data volume). Azure's own *Metrics*
page watches the **VM's health** (CPU, memory, network). The two complement each other.

### The controls at the top of the Grafana page

| Element | Meaning |
|---|---|
| **Grafana logo / ☰ menu** | Navigation (dashboards, explore, etc.) |
| **Home** | You're on the home dashboard, which is set to our *CDC Pipeline* dashboard |
| **Search or jump to…** | Find dashboards or panels |
| **Sign in** | Only needed to **edit**. Viewing works without login (anonymous read-only) |
| **🕘 Last 15 minutes** | Time range shown on the X-axis. You can change it to last 5 min, 1 h, etc. |
| **🔍− (zoom out)** | Widens the time range |
| **🔄 5s** | Auto-refresh: the dashboard redraws every 5 seconds |
| **ⓘ next to a panel title** | Hover to see that panel's description |
| **Hovering over a graph** | A tooltip shows the exact value of every line at that moment |

### How to read any graph
- **X-axis = time** (the last 15 minutes; the right edge is *now*).
- **Y-axis = the measured value.**
- **Each colour = one series**, named in the legend below the graph (a table, a percentile, …).
- Flat line = nothing happening. Spike = activity (e.g. when you clicked *Generate 20 orders*).
- **"/s" or "ops/s"** means *per second*, calculated as an **average over the last minute**
  (PromQL `rate(...[1m])`). So the number rises and falls smoothly rather than jumping.

### Top row: 4 big numbers (current values)

#### ① Change events / s
- **What:** how many database changes per second the consumer is applying to Redis. The initial
  snapshot (`op = r`) is excluded, so only *real* new changes are counted.
- **Example:** 3.5 ops/s after *Generate 20 orders*: about 180–200 events spread over a 1-minute average.
- **Good value:** 0 when idle, spikes when there's activity.

#### ② Commit → Redis (p95)
- **What:** the end-to-end delay from **PostgreSQL COMMIT** to the change **being in Redis**.
  Calculated as `now − commit time (ts_ms)` for each event.
- **p95** = 95th percentile: **95% of changes were faster than this**. It ignores the rare slowest 5%.
- **Example:** 243 ms with single orders; 435 ms during the 20-order burst on a 2-CPU VM.
- **Good value:** well under 1 second. This is the "eventual" in *eventual consistency*.

#### ③ Kafka consumer lag ← most important
- **What:** the number of events that are **already in Kafka but not yet applied to Redis**
  (Kafka's newest position minus the consumer's position).
- **Analogy:** the number of people waiting in a queue at a cash counter.
- **Example:** **0**, so Redis is fully caught up with PostgreSQL.
- **Good value:** 0, or a brief blip that returns to 0. A line that keeps climbing would mean
  the consumer is too slow.

#### ④ Redis hit rate
- **What:** of all Redis lookups, the share that **found** the key: `hits ÷ (hits + misses)`.
- **Example:** 99.3–99.6%, so almost every read was answered directly from the read model.
- The few misses come from normal checks (e.g. the website polling for a new order a few
  milliseconds before it arrives).

### Graphs (values over time)

#### ⑤ Change events per second, by table
- Same as ① but **split by table** (one colour per table): `orders`, `order_items`, `payments`,
  `order_status_history`, `products` (and `customers` / `categories` if they change).
- **Why several lines move together:** one order changes **5 tables** in one transaction (Section 5).
- **Example:** at 14:21, every line jumps together. That's the 20 generated orders.

#### ⑥ Commit → Redis latency (p50 / p95 / p99)
- **p50 (median):** the typical change, faster than half of all changes.
- **p95:** 95 out of 100 changes were faster than this.
- **p99:** 99 out of 100 changes were faster than this (the near-worst case).
- **Analogy:** parcel delivery. Most arrive in 2 days (p50); 1 in 100 takes 5 days (p99).
- **Example:** p50 ≈ 150–200 ms, p99 ≈ 500 ms during the burst. It rises under load and falls when quiet.

#### ⑦ Kafka consumer lag, by table
- Same as ③, split per table. A **flat line at 0** for every table means no backlog anywhere, even
  during bursts.

#### ⑧ Redis hit rate (over time)
- Same as ④ as a line. Staying near **100%** the whole time means reads are consistently served from Redis.

#### ⑨ Read-model views rebuilt per second
- How often the consumer rebuilds a ready-made document, split by type:
  - **order:** an order's full document (`view:order:<id>`)
  - **customer:** a customer's page (`view:customer:<id>`), rebuilt when they get a new order
  - **product:** a product page (`view:product:<id>`), rebuilt when stock changes
  - **order_deleted:** an order view removed because the order was deleted
- **Example:** the Earbuds + Smartwatch order rebuilds 1 order view, 1 customer view and 2 product views.

#### ⑩ Keys in the Redis read model
- The **total number of keys** stored in Redis.
- **Example:** it steps from ~13,660 to ~13,860 when 20 orders are generated: new order views, raw
  rows, items, payments, history and index entries. Redis grew **by itself**, because the app never
  writes to it.

### One-sentence summary of the dashboard
> When 20 orders came in, the changes flowed through instantly (events spiked), reached Redis within
> half a second (latency), nothing piled up (lag stayed at 0), reads kept hitting Redis (~99%) and
> Redis grew with the new data (keys went up). The pipeline is **fast, complete and in sync**.

---

## 7. How to start, demo and stop

### Start (≈ 5 minutes before the demo)
1. **portal.azure.com** → Resource groups → `cdc-pipeline-rg` → `cdc-pipeline-vm` → **Start**.
2. Wait **3–4 minutes**. Docker starts with the VM and restarts all containers automatically.
3. Check: VM → **Operations → Run command → RunShellScript** → paste and **Run**:
   ```bash
   cd /opt/cdc && sudo docker compose ps
   ```
   Everything should be **Up**; `cdc-connect-init` shows **Exited (0)** (normal).
   (Type commands only. Typing a container name like `cdc-connect-init` alone gives "not found".)

Other useful Run-command checks:
```bash
curl -s localhost:8083/connectors/shopdb-connector/status   # Debezium: expect "RUNNING" twice
sudo docker logs --tail 20 cdc-consumer                       # consumer's recent activity
sudo docker logs cdc-connect-init                             # connector registration log
```

### Demo
| Open | URL |
|---|---|
| Website | http://20.214.184.171:8080 |
| Grafana | http://20.214.184.171:3000 (no login needed) |

1. **Azure portal tour:** resource group → registry repositories (8 images) → VM overview →
   networking rules → Run command `docker compose ps`.
2. **Shop:** place an order and read out the journey and milliseconds.
3. **Orders:** *Track this order* → *Mark as processing*.
4. **How it works:** *Generate 20 orders* → switch to Grafana and explain the panels (Section 6).
5. **Run check** (PostgreSQL = Redis) → **Dual-write demo:** *1. Naive write*, then *2. Naive write + crash*.

### Stop (after the demo, to save credit)
VM page → **Stop** → confirm. The public IP stays the same. Next time just click **Start**.

Delete everything permanently (only when the project is completely over):
```bash
az group delete -n cdc-pipeline-rg --yes
```

---

## 8. Questions a professor may ask

| Question | Answer |
|---|---|
| What is CDC? | Capturing every committed change from the database's log and streaming it to other systems. |
| Why not just write to Redis from the app? | That's a dual write: a crash between the two writes leaves the cache wrong forever. With CDC the app writes once, and Redis is derived from the database log. |
| Is Redis always up to date? | Eventually consistent: typically 100–500 ms behind. *Run check* proves both stores match. |
| What if a Kafka broker fails? | Replication factor 3 and min in-sync replicas 2: any one broker can fail with no data loss. |
| What if the consumer crashes? | It restarts and re-reads from its last committed offset. Writes are idempotent, so there are no duplicates. |
| What if Debezium restarts? | The replication slot remembers its position in the WAL, so no change is skipped. |
| Why Redis for reads? | One GET of a pre-joined document instead of a 6-table JOIN, and reads never load PostgreSQL. |
| How do you prevent overselling? | `SELECT … FOR UPDATE` row locks + `CHECK (stock >= 0)` inside one ACID transaction. |
| What is containerization? | Packaging each component with all its dependencies into an image that runs identically anywhere (Section 2). |
| Docker image vs container? | Image = the frozen package; container = a running instance of it. |
| Why Azure Container Registry? | A private image store in the same cloud; the VM pulls versioned images from it. |
| Why a VM, not Kubernetes? | Kafka needs all brokers on one shared network; Compose on one VM is the simplest and cheapest fit for a student subscription. `k8s/` holds manifests for scaling up. |
| How is it secured? | Firewall allows only 3 ports; DB, Kafka and Redis are internal; least-privilege DB roles; private registry; changed Grafana admin password. |
| What does p95 mean? | 95% of changes were faster than this value. |
| What does consumer lag 0 mean? | No events are waiting: Redis has everything PostgreSQL committed. |

---

## 9. Glossary

| Term | Simple meaning |
|---|---|
| **ACID** | Atomic, Consistent, Isolated, Durable: the guarantees of a database transaction |
| **ACR** | Azure Container Registry, a private store for Docker images |
| **At-least-once** | Every event is processed one or more times, never zero |
| **Broker** | One Kafka server |
| **CDC** | Change Data Capture |
| **cloud-init** | A script that runs automatically on a cloud VM's first boot |
| **Consumer group / offset** | Kafka's way of remembering how far a consumer has read |
| **CQRS** | Separate models for writing (commands) and reading (queries) |
| **Denormalized view** | Data stored pre-joined for fast reading |
| **Docker Compose** | Runs many containers from one YAML file |
| **Eventual consistency** | The copy catches up with the source after a short delay |
| **Idempotent** | Doing it twice has the same effect as doing it once |
| **Image / container** | Frozen package / running instance of it |
| **Kafka topic / partition** | A named event stream / a slice of it processed in order |
| **NSG** | Network security group, Azure's firewall |
| **p50 / p95 / p99** | Percentiles: 50 / 95 / 99% of values are below this |
| **Prometheus** | A time-series database that scrapes metrics |
| **PromQL** | Prometheus' query language, used by Grafana |
| **Replication factor** | How many brokers keep a copy of each event |
| **Replication slot** | PostgreSQL's bookmark for a CDC reader in the WAL |
| **Resource group** | A folder of related Azure resources |
| **Trigger** | Code the database runs automatically on insert/update/delete |
| **WAL** | Write-ahead log: PostgreSQL's journal of every change |
| **ZooKeeper** | Coordinates the Kafka brokers |
