# Spark Structured Streaming Take-Home Assignment

## Overview

This assignment evaluates your **hands-on experience with Apache Spark Structured Streaming**, with a focus on:

* Event-time stream processing
* Stateful transformations
* Execution and performance awareness
* Reliability, checkpointing, and recovery

The emphasis is on **how you design and reason about a streaming pipeline**, not on infrastructure setup.

---

## Environment Constraints

* **Execution environment:** Local machine only
* **Platform requirements:** Docker (Kafka is preconfigured)

---

## Getting Started: Running the Docker Setup

### Prerequisites

* Docker and Docker Compose installed
* Python 3.7+ (for Spark job execution)

### Starting Kafka & Producer

Navigate to the project directory and start all services:

```bash
docker compose up -d --build
```

This will start:
* **Zookeeper** on `localhost:2181`
* **Kafka** on `localhost:9092`
* **Kafka Producer** (auto-generates order events to `order_events` topic)
* **Kafka Consumer** (displays consumed events in real-time)

### Verify Services Are Running

```bash
# Check container status
docker compose ps

# View Kafka logs
docker compose logs -f kafka

# View producer logs
docker compose logs -f kafka-producer

# View consumer logs
docker compose logs -f kafka-consumer
```

### Stopping Services

```bash
docker compose down -v
```

---

## What Is Already Provided

The repository contains:

* A **Docker Compose setup** that starts Kafka locally
* A **Kafka producer** that publishes order events
* A Kafka topic named: `order_events`

You do **not** need to modify:

* Kafka configuration
* Docker Compose files
* Producer logic

---

## Input Data Description

Kafka messages are JSON-encoded and follow this schema:

```json
{
  "order_id": "string",
  "customer_id": "string",
  "product_id": "string",
  "event_type": "CREATED | UPDATED | CANCELLED",
  "quantity": integer,
  "price": double,
  "event_time": "ISO-8601 timestamp"
}
```

### Important Characteristics

* Events may arrive **out of order**
* Duplicate events may occur
* Multiple events can exist for the same `order_id`

---

## Task: Implement a Spark Structured Streaming Pipeline

You are required to implement a **single Spark Structured Streaming application** that satisfies **all** of the following requirements.

---

### 1. Streaming Ingestion & Parsing

Your application must:

* Read events from Kafka topic `order_events`
* Safely parse JSON payloads
* Use **event-time semantics** (not processing time)

---

### 2. Stateful Processing & Correctness

Your application must:

* Deduplicate events correctly
* Handle late-arriving data using an appropriate watermark
* Maintain the **latest state per `order_id`**
* Correctly process `CREATED`, `UPDATED`, and `CANCELLED` events

---

### 3. Aggregations

Compute and persist the following analytics:

* **Total order value per `customer_id`** using **5-minute tumbling event-time windows**
* **Count of cancelled orders per window**

---

### 4. Output & Storage

* Write results to local storage (Parquet or Delta)
* Choose an appropriate output mode
* Partition data sensibly for downstream consumption

---

### 5. Execution Awareness & Optimization (Mandatory)

Your solution must demonstrate **conscious execution-level decisions**, including:

* Watermark configuration and rationale
* Trigger interval selection
* State growth control strategy
* Shuffle minimization
* Partitioning strategy

These choices **must be explained** in the README.

---

### 6. Reliability, Checkpointing & Recovery (Mandatory)

Your solution must:

* Use checkpointing
* Be **restartable without data loss**
* Handle duplicate Kafka messages safely
* Maintain correct results if the Spark application is **stopped and restarted**

You do not need to explicitly simulate failures, but your design must clearly support recovery.

---

## Deliverables

### 1. Source Code

* Spark Structured Streaming application
* Clear, readable, and maintainable structure

### 2. README.md (Mandatory)

Your README **must** include:

* How to run the Spark job
* High-level architecture
* Streaming semantics used
* Key design and execution decisions
* Optimization techniques applied
* Checkpointing and recovery approach
* Assumptions and trade-offs

---

## Submission Instructions

* Submit your solution as a **Git repository**
* Ensure the repository contains:

  * Source code
  * README.md
* The solution must be runnable using:

  * The provided Kafka setup
  * A local Spark installation or container