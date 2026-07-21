# Теоретические темы и технологический стек

## PostgreSQL AGE

* **Labeled Property Graph Model:** Understanding Vertices (Nodes), Edges (Relationships), and Properties. Identifying use cases where graph traversals vastly outperform relational JOIN operations.
* **Cypher Query Language:** Basic syntax of openCypher. Pattern matching, path finding, filtering, and aggregation in graph networks.
* **Apache AGE Specifics:** How the AGE extension integrates into the PostgreSQL ecosystem. Writing hybrid queries combining standard SQL and graph Cypher via the `agtype` function. Indexing graph properties.

## TimescaleDB (Postgres Extension)

* **Time-Series Data Concepts:** What time-series data is and why classical B-Tree indexes fail on terabyte-scale volumes.
* **TimescaleDB Architecture:** The concept of Hypertables and Chunks. How automatic partitioning works across time and space.
* **Storage Optimization:** Columnar compression mechanics (Compression Policies) in TimescaleDB. The inner workings and lifecycle of Continuous Aggregates vs. traditional Materialized Views.
* **Data Retention:** Configuring automated policies for deleting or moving historical data (Data Tiering).

## Kubernetes (K8s)

* **K8s Primitives for Data Storage:** Core differences between `Deployment` and `StatefulSet`.
* **Working with Persistent Storage:** PersistentVolume (PV), PersistentVolumeClaim (PVC), and StorageClass.
* **Autoscaling & Resource Management:** Configuring CPU/Memory Requests/Limits. Principles of the Horizontal Pod Autoscaler (HPA) using custom metrics (e.g., scaling based on Kafka lag/queue length).
* **Event-Driven Architecture (EDA) Patterns:** Implementing the Idempotent Consumer pattern (handling duplicate events), Circuit Breaker (safeguarding against external AI API downtime), and Dead Letter Queue (DLQ) for poisonous messages.

## Apache Kafka

* **Kafka Architecture:** Brokers, Controller, Topics, and Partitions. The mechanics of the Commit Log and Zero-copy data transfer.
* **Producers & Consumers:** Message distribution logic across partitions (Partitioning Strategies, Message Keys). Consumer Groups concept, Offset management, and the Rebalance process.
* **Delivery Semantics:** Configuring acknowledgments (`acks=0`, `1`, `all`). Scenarios for At-least-once, At-most-once, and Exactly-once (Idempotent & Transactional Producers) delivery.
* **Kubernetes Management:** Specifics of running Stateful applications in K8s. The role of operators (Strimzi) and the difference between ZooKeeper and Kafka Raft (KRaft) modes.

## AI (Claude LLM)

* **LLM API Integration:** The Stateless nature of interacting with LLMs. Core concepts of Tokens, Tokenization, and Context Window management.
* **Structured Prompt Engineering (System Prompts):** Forcing the model to return strictly valid JSON objects (JSON Mode / Tool Calling) without conversational filler.
* **Lightweight RAG (Retrieval-Augmented Generation):** Context enrichment principles. Effectively aggregating data from multiple sources (structured logs from TimescaleDB + network topologies from AGE) and fitting them into Claude's prompt token limits.
