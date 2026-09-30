# E-commerce Data Engineering Platform

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Celery](https://img.shields.io/badge/Celery-37814A?style=flat-square&logo=celery&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=flat-square&logo=redis&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat-square&logo=apache-kafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=flat-square&logo=postgresql&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=flat-square&logo=kubernetes&logoColor=white)

> A distributed data engineering platform for automatically collecting, processing, and storing product, price, and promotion data from multiple online stores.

The project started as a simple idea — collect promotions from online stores — but is intentionally being developed as a robust distributed system to explore scalable data engineering architectures, asynchronous processing, and container orchestration. The long-term goal is to evolve the project into a comprehensive **distributed e-commerce price intelligence platform**.

## Architecture & Data Flow

The platform operates as an asynchronous pipeline where components are loosely coupled and scale independently within a **k3s Kubernetes cluster**.

* **Task Scheduling & Distribution:** **Celery Beat** schedules periodic scraping jobs. These tasks are distributed across multiple **Celery workers** using **Redis** as a low-latency message broker.
* **Data Collection:** Independent, containerized Python scrapers fetch and normalize product data from various targets. 
* **Buffering & Streaming:** To prevent database bottlenecks and handle bursts of traffic, workers do not write directly to the database. Extracted data is published to **Apache Kafka**, acting as a highly available buffer and streaming layer.
* **Persistence & Processing:** Dedicated consumer applications read batched messages from Kafka and securely upsert the records into **PostgreSQL**. This step handles entity deduplication and preserves historical price changes efficiently.
* **Orchestration:** The entire ecosystem runs inside Kubernetes, providing automatic service recovery, horizontal scaling for workers, and seamless multi-node deployment.

## Extensibility (Adding New Stores)

The system is designed to keep individual data sources independent from the core pipeline. Adding a new store requires only three simple steps:

1. Implementing a Python scraper for the specific site.
2. Returning the scraped data in the standardized system schema.
3. Registering and scheduling the new Celery task.

## Future Development

The architecture sets a strong foundation for future data analytics and platform scaling. Planned features are categorized below:

**Data Analytics & Processing**
* Integration with **Apache Spark / PySpark** for large-scale historical data analysis.
* Cross-store price comparison and product matching (entity resolution).
* Price trend analysis, anomaly detection, and advanced data quality validation.

**Platform Features**
* Real-time price monitoring and alerts/notifications for price drops.
* Support for a wider variety of e-commerce platforms.
* Complete and accessible price history for individual products.

**Infrastructure & Observability**
* **Prometheus & Grafana** integration for monitoring pipeline health, metrics, and visualization dashboards.
* Automatic, metrics-driven horizontal worker scaling based on queue size.
