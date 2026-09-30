# E-commerce Data Engineering Platform

A distributed data engineering platform for automatically collecting, processing, and storing product, price, and promotion data from online stores.

The project started as a simple idea — **collect promotions from online stores** — but instead of building a simple scraper, it is intentionally being developed as a more complex distributed system to explore modern data engineering technologies, scalable architectures, asynchronous processing, and container orchestration.

## Overview

The platform is designed to:

- Collect product and pricing data from multiple online stores
- Process data concurrently using multiple workers
- Schedule scraping jobs automatically
- Buffer and asynchronously process collected data
- Reduce database overhead through batch processing
- Prevent duplicate product data
- Store historical price information
- Recover services after failures
- Scale horizontally across multiple machines
- Provide a foundation for further data analysis and visualization

The long-term goal is to evolve the project from a promotion scraper into a **distributed e-commerce price monitoring and analytics platform**.

## Tech Stack

<p align="center">
  <img src="https://skillicons.dev/icons?i=python,docker,redis,postgres,kafka,kubernetes,grafana,prometheus" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Celery-37814A?style=for-the-badge&logo=celery&logoColor=white" />
  <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" />
</p>

| Technology | Purpose |
|---|---|
| **Python** | Scraping & data processing |
| **Celery** | Distributed task processing |
| **Redis** | Task broker |
| **Apache Kafka** | Data streaming & buffering |
| **PostgreSQL** | Data storage |
| **Docker** | Containerization |
| **k3s / Kubernetes** | Orchestration & scaling |
| **Apache Spark / PySpark** | Large-scale data processing |
| **Prometheus** | Monitoring |
| **Grafana** | Metrics & visualization |

## Data Collection

Each supported online store has its own scraper responsible for collecting and normalizing product data into a common format.

Adding a new store should require only:

1. Implementing a scraper
2. Returning data in the expected format
3. Creating and scheduling the appropriate task

This keeps individual data sources independent from the rest of the system.

## Celery & Redis

Scraping tasks are distributed across multiple **Celery workers**, allowing data to be collected concurrently from multiple stores.

**Redis** acts as the task broker, while **Celery Beat** handles periodic job scheduling.

## Apache Kafka

Collected data is published to **Kafka** instead of being written directly to the database.

Kafka acts as a buffer between data collection and persistence, allowing data to be processed and inserted into the database in batches.

## PostgreSQL

**PostgreSQL** is used as the primary database for storing products, stores, offers, prices, and historical data.

The system is designed to avoid duplicate product records while preserving price history.

## k3s / Kubernetes

The platform runs as a collection of containerized services inside a **k3s Kubernetes cluster**.

Kubernetes provides:

- Automatic service recovery
- Container orchestration
- Horizontal scaling
- Multi-node deployment
- Easier management of distributed components

Additional worker replicas or cluster nodes can be added as the workload grows.
The architecture allows the number of workers to be increased independently of the rest of the system and can be distributed across multiple k3s nodes.

## Future Development

Possible future features include:

- Support for additional online stores
- Complete price history
- Real-time price monitoring
- Cross-store price comparison
- Product matching and entity resolution
- Price trend analysis
- Anomaly detection
- Data visualization dashboard
- Prometheus & Grafana monitoring
- Automatic worker scaling
- Apache Spark / PySpark processing
- Large-scale historical data analysis
- Price change notifications
- Advanced data quality validation

The long-term vision is to transform the project from a simple promotion collector into a distributed e-commerce price intelligence platform.
