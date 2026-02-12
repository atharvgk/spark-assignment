# Spark Structured Streaming - Order Events Pipeline

A production-ready Spark Structured Streaming application that processes order events from Kafka with event-time semantics, stateful processing, and windowed aggregations.

## How to Run

### 1. Starting Kafka & Producer

Navigate to the project directory and start all services:

```bash
docker compose up -d --build
```

This will start:
* **Zookeeper** on `localhost:2181`
* **Kafka** on `localhost:9092`
* **Kafka Producer** (auto-generates order events to `order_events` topic)
* **Kafka Consumer** (displays consumed events in real-time)

### 2. Verify Services Are Running

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

### 3. Stopping Services

```bash
docker compose down -v
```

### 4. Running the Spark Job

Open a new terminal, enter WSL, and execute the following sequence:

1.  Run the Spark application:
    ```bash
    ./run_spark.sh
    ```
2.  Let it run for a while, then stop it with `Ctrl+C`.
3.  Verify the output directory structure:
    ```bash
    ./verify_output.sh
    ```
4.  Verify the Parquet file content:
    ```bash
    /opt/spark/bin/spark-submit verify_parquet.py
    ```

---

## High-Level Architecture


### Data Flow

1.  **Ingestion**: Read JSON events from Kafka topic `order_events`
2.  **Parsing**: Parse JSON with strict schema validation
3.  **Watermarking**: 10-minute watermark for late data handling
4.  **Deduplication**: Remove exact duplicates based on `order_id` + `event_time`
5.  **Processing**:
    *   **Stream 1**: Maintain latest state per `order_id` (handles CREATED → UPDATED → CANCELLED)
    *   **Stream 2**: Compute 5-minute tumbling window aggregations
6.  **Output**: Write to Parquet with separate checkpoints per stream

---

## Streaming Semantics Used

### Event-Time Processing
*   **Event Time**: Uses `event_time` field from Kafka messages (ISO-8601 timestamp)
*   **Processing Time**: Only used for triggering micro-batches (10 seconds)
*   **Watermark**: 10 minutes to handle out-of-order events

### Exactly-Once Semantics
*   **Kafka Source**: `startingOffsets=earliest` with offset tracking
*   **Checkpointing**: Separate checkpoints per query for fault tolerance
*   **Idempotent Writes**: Parquet sink with checkpoint-based deduplication

### Stateful Operations
1.  **Deduplication**: `dropDuplicates(["order_id", "event_time"])` with watermark
2.  **Latest State**: Window function to keep most recent event per `order_id`
3.  **Windowed Aggregations**: 5-minute tumbling windows with watermark

---

## Key Design and Execution Decisions

### 1. Dual Output Streams
**Rationale**: The task requires both maintaining the latest state per `order_id` (operational) and computing windowed aggregations (analytics).
**Implementation**: Separate queries allow for independent checkpoint management and clearer separation of concerns.

### 2. Watermark Configuration (10 Minutes)
**Rationale**: The producer generates events with a time skew of -120 to +30 seconds. A 10-minute watermark provides a safe buffer for network delays and processing lag while keeping state memory usage manageable.

### 3. Deduplication Strategy
**Approach**: `dropDuplicates(["order_id", "event_time"])`
**Rationale**: The producer intentionally sends ~10% duplicates. We deduplicate based on both ID and time to ensure we don't process the exact same event twice.

### 4. Latest State Management
**Approach**: Window function with `row_number()` ordered by `event_time DESC`
**Rationale**: This effectively handles state transitions (CREATED → UPDATED → CANCELLED) by ensuring we always capture the most recent update for an order within the micro-batch.

---

## Optimization Techniques Applied

### 1. Shuffle Minimization
*   **Pre-filtering**: We filter data (e.g., separating Cancelled orders) *before* aggregation to reduce the amount of data shuffled.
*   **Coalesce**: Reduced shuffle partitions to 4, which is appropriate for a local execution environment to avoid creating too many small tasks.

### 2. Trigger Interval (10 Seconds)
**Rationale**: Balances latency and throughput. A 10-second interval allows multiple events to accumulate, improving batch efficiency and reducing checkpointing overhead compared to default (continuous) processing.

### 3. State Growth Control
**Mechanisms**: Watermarking automatically evicts old state. Deduplication and window aggregations are also bounded by the watermark, preventing unbounded state growth.

---

## Checkpointing and Recovery Approach

### Strategy
We use **Checkpointing** for all streaming queries. This saves the query progress (Kafka offsets) and the running state (aggregations, deduplication info) to a fault-tolerant storage location (local disk in this case).

### Recovery Guarantees
*   **Restartable**: If the application crashes or is stopped, it can be restarted and will resume exactly where it left off.
*   **No Data Loss**: Kafka offsets ensure no messages are skipped.
*   **No Duplicates**: State restoration ensures we don't re-process already handled data.

### Checkpoint Structure
```
./output/checkpoints/
├── latest_orders/          # Stream 1
├── aggregations/
│   ├── customer_value/     # Stream 2a
│   └── cancelled_orders/   # Stream 2b
```

---

## Assumptions and Trade-offs

### Assumptions
1.  **Cancelled Orders**: These should not contribute to the "Total Order Value" metric.
2.  **Late Data**: Events arriving more than 10 minutes late are acceptable to drop.
3.  **Ordering**: The `event_time` field is the source of truth for event ordering.

### Trade-offs
*   **Latency vs. Throughput**: We chose a 10-second trigger. This adds up to 10 seconds of latency but significantly improves throughput and reduces overhead.
*   **State Size vs. Late Data**: The 10-minute watermark means we hold state for 10 minutes. Increasing this would handle more late data but increase memory usage.
