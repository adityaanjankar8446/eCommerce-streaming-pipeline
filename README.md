# Ecommerce Streaming Data Pipeline

## Project Overview

This project implements a real-time ecommerce analytics pipeline using:

- Python
- Apache Spark Structured Streaming
- Delta Lake
- Apache Airflow
- Docker

End-to-end streaming data pipeline built with Apache Spark, Delta Lake, Apache Kafka, and Apache Airflow — running fully locally via Docker.
The pipeline continuously ingests ecommerce data from APIs, processes it through Bronze, Silver, and Gold layers, and produces business-ready analytics datasets.


---

## Architecture

FakeStore API
↓
Python Ingestion
↓
Raw JSON Files
↓
Bronze Layer
↓
Silver Layer
↓
Gold Layer
↓
Analytics

---

## Technology Stack

| Component | Technology |
|------------|------------|
| Source | FakeStore API |
| Ingestion | Python |
| Processing | Apache Spark |
| Storage | Delta Lake |
| Orchestration | Airflow |
| Containerization | Docker |

---

## Pipeline Layers

### Bronze Layer

- Schema enforcement
- JSON flattening
- Array explosion
- Raw Delta storage

### Silver Layer

- Data cleansing
- Type casting
- Standardization
- Business enrichment

### Gold Layer

- Customer Revenue Analytics
- Product Performance Analytics
- Category Revenue Analytics

---

## Setup & Run

### 1. Start all services
```bash
docker-compose up -d
```

### 2. Create folder structure
```bash
mkdir data\raw\orders data\raw\customers data\raw\products
mkdir data\delta\bronze data\delta\silver data\delta\gold
mkdir checkpoints dags scripts
```

### 3. Fix permissions
```bash
docker exec -it --user root spark-master chmod -R 777 /opt/data/
docker exec -it --user root spark-master chmod -R 777 /opt/checkpoints/
docker exec -it --user root airflow-scheduler mkdir -p /opt/checkpoints
docker exec -it --user root airflow-scheduler chmod -R 777 /opt/checkpoints
docker exec -it --user root airflow-scheduler chmod -R 777 /opt/data
```

### 4. Install Java in Airflow
```bash
docker exec -it --user root airflow-scheduler apt-get update
docker exec -it --user root airflow-scheduler apt-get install -y default-jdk
```


### 5. Run pipeline manually
```bash
# Ingest API data
python scripts/api_ingest.py

# Bronze layer
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  /opt/scripts/01_bronze.py

# Silver layer
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  /opt/scripts/02_silver.py

# Gold layer
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  /opt/scripts/03_gold.py



## Tech Stack

| Tool           | Version | Purpose |
|---             |---      |---|
| Apache Spark   | 3.5.1 | Stream processing |
| Delta Lake     | 3.1.0 | ACID storage |
| Apache Kafka   | 7.5.0 | Message broker |
| Apache Airflow | 2.7.2 | Orchestration |
| Docker         | latest| Containerization |
| Python         | 3.11  | Scripting |


```
```

### 6. Access UIs
| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8081 | admin/admin |
| Spark Master | http://localhost:8080 | - |
| Kafka UI | http://localhost:8085 | - |


## Pipeline Layers

### Bronze
Raw data ingested from API. Schema enforced, JSON flattened, arrays exploded. Stored as Delta tables with checkpointing.

### Silver  
Cleaned and enriched data. Timestamps cast, names standardized, emails validated, price categories added.

### Gold
Business aggregations. Revenue by customer, popular products, revenue by category.

## Airflow DAG
Scheduled hourly via cron `0 * * * *`. Tasks run sequentially:
`api_ingest → bronze_ingestion → silver_transformation → gold_aggregations`