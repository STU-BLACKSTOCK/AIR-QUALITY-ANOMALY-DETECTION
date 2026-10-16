# Teacher Demo & Presentation Walkthrough Guide
## Real-Time Air Quality Anomaly Detection Pipeline

This guide outlines the exact, step-by-step procedure to demonstrate the project to your teacher or evaluator tomorrow, including the exact terminal commands, screen navigation, code explanations, and viva answers.

---

## 1. Morning Pre-Demo Checklist (5 Minutes Before Presentation)

1. **Start Docker Desktop**:
   - Open **Docker Desktop** from your Windows Start Menu.
   - Wait 15–30 seconds until the bottom-left icon turns **green** with **"Engine running"**.
2. **Open Terminal (PowerShell)**:
   - Open your project directory:
     ```powershell
     cd C:\project\BDA_Project\realtime-air-quality-anomaly-detection-main
     ```
3. **Allow Script Execution**:
   - Run this once in your PowerShell session:
     ```powershell
     Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
     ```
4. **Prepare 4 Terminal Tabs or Windows**:
   - **Tab 1**: Cluster Management & Tests (Commands, HDFS inspection)
   - **Tab 2**: Spark Streaming & Alert Handler (`alerts/alert_handler.py`)
   - **Tab 3**: Live Web Dashboard Server (`dashboard/app.py`)
   - **Tab 4**: Live Kafka Stream Producer (`producer/producer.py`)

---

## 2. The 5-Minute Live Demo Flow (Step-by-Step)

Follow this sequence in front of your teacher:

### Step 1: Start & Explain the Distributed Cluster (Tab 1)
```powershell
.\scripts\start.ps1
```
> **What to tell your teacher**:
> *"We have deployed a 7-service distributed big data infrastructure using Docker Compose: Apache Kafka with ZooKeeper for high-throughput stream buffering, Apache Hadoop HDFS with NameNode and DataNode for distributed fault-tolerant storage, and an Apache Spark Master and Worker cluster for distributed stream processing."*

Open these links in your browser to show the teacher:
- **Spark Master UI**: [http://localhost:8085](http://localhost:8085) *(Show 0 running applications initially, 1 worker active)*
- **HDFS NameNode UI**: [http://localhost:9870](http://localhost:9870) *(Click 'Utilities' -> 'Browse the file system' to show the `/air_quality` root)*

---

### Step 2: Show Automated Test Suite & Preprocessing (Tab 1)
Run the test suite:
```powershell
python -m pytest tests/
```
**Output**: `11 passed in ~1.3s`
> **What to tell your teacher**:
> *"Before streaming, we verified our data pipeline with 11 automated unit and integration tests. These validate our handling of real-world IoT sensor quirks: replacing semicolon delimiters, parsing comma-decimals (`2,6` -> `2.6`), filtering `-200` sentinel error values, computing statistical baselines, and verifying Kafka end-to-end roundtrips."*

---

### Step 3: Launch the Spark Structured Streaming Anomaly Detector (Tab 1)
Submit the PySpark job to the cluster:
```powershell
docker exec -d bda-spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --master spark://spark-master:7077 /workspace/streaming/anomaly_detector.py
```
> **What to tell your teacher**:
> *"The Spark streaming application is now running on the Spark Master. It continuously listens to the Kafka topic `air-quality-raw`, executes a 10-minute watermark for out-of-order sensor packets, calculates Z-Scores in $O(1)$ time per record, and evaluates WHO/EPA environmental safety limits."*

Refresh [http://localhost:8085](http://localhost:8085) to show `AirQualityAnomalyDetector` is now listed under **Running Applications**.

---

### Step 4: Launch Web Dashboard & Real-Time Alert Handler

**In Tab 2 (Alert Consumer)**:
```powershell
python alerts/alert_handler.py
```
*(Leave this running — it will listen on topic `air-quality-alerts`)*

**In Tab 3 (FastAPI Dashboard)**:
```powershell
python dashboard/app.py
```
Open **[http://localhost:8000](http://localhost:8000)** in your browser:
- Show the dark-mode glassmorphic dashboard.
- Point out the **Baseline References**, **SLA Latency Benchmark** counters, and the live telemetry feed.

---

### Step 5: Start Sensor Replay with Synthetic Pollution Spikes (Tab 4)
In Tab 4, stream 60 sensor records with spike injection every 10 records:
```powershell
python producer/producer.py --limit 60 --interval 0.1 --inject-spikes --spike-interval 10
```

**Now show the teacher the magic happening simultaneously**:
1. **In Tab 2**: Watch formatted terminal alert cards flash in real-time (`WARNING`, `CRITICAL`, `EMERGENCY`) showing the pollutant, value, and standard deviations from the mean.
2. **In the Web Browser ([http://localhost:8000](http://localhost:8000))**:
   - Watch the **Server-Sent Events (SSE)** feed display incoming anomaly alerts instantly.
   - Click the blue **"⚡ Inject Synthetic Spike"** button on the UI: show how an emergency alert immediately appears within seconds!

---

### Step 6: Prove Persistence in Hadoop HDFS (Tab 1)
Show that the big data sink actually persisted data to distributed storage:
```powershell
docker exec bda-namenode hdfs dfs -ls -R /air_quality
```
> **What to point out to your teacher**:
> 1. `/air_quality/parquet/raw/date=YYYY-MM-DD/`: *"All sensor readings are partitioned by date and compressed with Snappy in columnar Parquet format for efficient analytical batch querying."*
> 2. `/air_quality/parquet/anomalies/`: *"Flagged toxic anomalies are isolated into a dedicated Parquet path for environmental compliance auditing."*
> 3. `/air_quality/checkpoints/`: *"Spark Structured Streaming Write-Ahead Logs (WAL) commit offsets here, guaranteeing exactly-once processing even if a worker crashes."*

---

### Step 7: Run the Latency Benchmark & SLA Verification (Tab 1)
Run the automated benchmark:
```powershell
python tests/verify_latency.py
```
> **What to show**:
> The test injects deterministic spikes with unique IDs and measures $T_{\text{alert}} - T_{\text{ingest}}$.
> Point out the results:
> - **Observed Latency**: **~3 to 6 seconds**
> - **University Project SLA Requirement**: `< 300 seconds` (5 minutes) -> **PASSED**
> - **Strict Production Target**: `< 60 seconds` -> **PASSED**

---

### Step 8: Tear Down After Presentation (Tab 1)
```powershell
.\scripts\stop.ps1
```

---

## 3. Code Explanations: What to Show Your Teacher

If your teacher says: *"Open your code and show me how you implemented..."*:

### A. "How did you clean the raw dataset and handle -200 sentinels?"
Open `preprocessing/clean.py` and highlight lines 80–135:
```python
# 1. Clean comma decimals and unify timestamp
datetime_str = df["Date"] + " " + df["Time"]
df["timestamp"] = pd.to_datetime(datetime_str, format="%d/%m/%Y %H:%M:%S")

# 2. Convert -200 sentinels (sensor failure values) to NaN
df.loc[df[col] <= -200, col] = np.nan

# 3. Impute missing values using forward-fill (LOCF) for atmospheric continuity
df_imputed[cols_to_fill] = df_imputed[cols_to_fill].ffill().bfill()
```
*Explanation: "In atmospheric physics, sensor readings have high inertia. Forward-filling (Last Observation Carried Forward) maintains continuous diurnal curves without causing artificial mean shifts."*

---

### B. "Where is the statistical anomaly detection logic in PySpark?"
Open `streaming/anomaly_detector.py` and highlight lines 124–165:
```python
# 1. 10-Minute Watermarking for handling delayed network IoT packets
stream_df = parsed_df.withWatermark("parsed_timestamp", "10 minutes")

# 2. Z-Score Computation: Z = |x - mean| / std
scored_df = stream_df \
    .withColumn("z_CO", sql_abs(col("CO_GT") - lit(mu_co)) / lit(std_co)) \
    .withColumn("z_C6H6", sql_abs(col("C6H6_GT") - lit(mu_c6h6)) / lit(std_c6h6))

# 3. Hybrid Anomaly Rule: Statistical Outlier (|Z| >= 3.0) OR Regulatory Hard Limit
is_co_anomaly = (col("z_CO") >= 3.0) | (col("CO_GT") >= hard_co)
is_anomaly = is_co_anomaly | is_c6h6_anomaly | is_nox_anomaly | is_no2_anomaly
```
*Explanation: "We use a hybrid detection engine: Z-Scores catch sudden relative spikes even if background levels are low ($|Z| \ge 3.0$ is $p < 0.003$), while WHO/EPA hard thresholds prevent missing absolute toxic levels during prolonged stagnation."*

---

### C. "How did you store data to HDFS in Parquet format?"
Open `streaming/anomaly_detector.py` and highlight lines 195–203:
```python
raw_hdfs_query = annotated_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/air_quality/parquet/raw") \
    .option("checkpointLocation", "hdfs://namenode:9000/air_quality/checkpoints/raw_parquet") \
    .partitionBy("date") \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .start()
```
*Explanation: "We use Spark's streaming Parquet sink partitioned by `date`. Checkpointing offsets directly to HDFS ensures write-ahead logging and zero data loss on node crashes."*

---

## 4. University Viva Q&A Cheat Sheet

| Question | Short, Impressive Answer to Give |
| :--- | :--- |
| **Why Kafka over RabbitMQ?** | *"Kafka is an immutable, distributed commit log with partition-based pub/sub. Unlike RabbitMQ which deletes messages on delivery, Kafka persists sensor streams for a configurable retention window. This allows our Spark streaming engine, alert consumer, and dashboard to independently consume at different speeds without dropping packets."* |
| **Why Spark Structured Streaming over Nightly Batch?** | *"Nightly batch ETL introduces a 3 to 12 hour delay before reporting pollution spikes, meaning citizens are exposed before advisories are issued. Our Spark Structured Streaming pipeline operates on micro-batches of 5–10 seconds, reducing alert latency from hours down to ~4 seconds."* |
| **What is Watermarking?** | *"IoT sensors often transmit packets with delays due to wireless network lag. A 10-minute watermark instructs Spark to keep an in-memory sliding state window for 10 minutes of event time to aggregate late arrivals. Once event time moves beyond 10 minutes, old state is purged to prevent unbounded memory exhaustion."* |
| **Why Parquet in HDFS instead of CSV or JSON?** | *"Parquet is columnar and snappy-compressed. It supports column pruning (e.g. reading only `CO_GT` without scanning 14 other columns) and predicate pushdown. It saves over 70% storage space compared to raw CSV and accelerates historical BI queries."* |
| **How is Fault Tolerance guaranteed?** | *"Through Write-Ahead Logging (WAL) and checkpointing in HDFS (`/air_quality/checkpoints`). Every micro-batch commits source Kafka offsets and sink state transactions. If an executor or container crashes, Spark restarts from the exact offset in HDFS without message duplication."* |
| **How did you measure Latency?** | *"We embedded three timestamps: sensor `event_time`, producer `ingest_time` (Kafka arrival), and Spark `detection_time`. End-to-end latency is $T_{\text{alert}} - T_{\text{ingest}}$. In our automated test `verify_latency.py`, the end-to-end turnaround was verified at 2.9 to 6.2 seconds, comfortably beating the 5-minute requirement."* |
