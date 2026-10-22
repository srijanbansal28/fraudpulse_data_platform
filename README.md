# FraudPulse Data Platform

Financial transaction anomaly detection pipeline using Python, PySpark, Delta Lake, and Databricks.

## Architecture Overview

```
┌──────────────┐     ┌──────────────┐     ┌──────────────────────────────────────┐
│   Producer   │────▶│  AWS Kinesis  │────▶│         Databricks Lakehouse         │
│  (Python)    │     │  Data Stream  │     │                                      │
└──────────────┘     └──────────────┘     │  ┌────────┐  ┌────────┐  ┌────────┐  │
                                          │  │ Bronze │─▶│ Silver │─▶│  Gold  │  │
┌──────────────┐     ┌──────────────┐     │  │ (Raw)  │  │(Clean) │  │ (Agg)  │  │
│  Batch Gen   │────▶│  Local       │     │  └────────┘  └───┬────┘  └────────┘  │
│  (Python)    │     │  Ingestion   │     │                  │                    │
└──────────────┘     └──────────────┘     │           ┌──────▼──────┐             │
                                          │           │  Anomaly    │             │
                                          │           │  Detection  │             │
                                          │           └─────────────┘             │
                                          └──────────────────────────────────────┘
```

## Project Structure

```
fraudpulse_data_platform-main/
├── producer/
│   ├── generate_events.py         # Generate 1000+ synthetic transactions (JSONL)
│   ├── generate_batch.py          # Generate timestamped batch files for ingestion
│   └── kinesis_producer.py        # Stream events to AWS Kinesis Data Streams
├── scripts/
│   └── incremental_ingestion.py   # Local batch ingestion with checkpointing
├── notebooks/
│   ├── 01_bronze_ingestion.py     # Bronze: raw JSONL → Delta with metadata
│   ├── 02_silver_transformations.py  # Silver: dedup, validate, enrich
│   ├── 03_anomaly_detection.py    # Anomaly: 5-rule risk scoring engine
│   └── 04_gold_aggregates.py      # Gold: 5 pre-computed aggregate tables
├── sql/
│   └── risk_analytics.sql         # 10 analytical queries for dashboards
├── data/
│   └── generated/
│       └── transactions.jsonl     # Sample synthetic transaction data
├── tests/
│   ├── test_batch_generator.py    # Batch generation tests
│   ├── test_fraudpulse.py         # Dedup and validation logic tests
│   └── test_incremental_ingestion.py  # Ingestion pipeline tests
├── requirements.txt
└── README.md
```

## Medallion Architecture (Databricks)

### Bronze Layer — Raw Ingestion
- Reads raw JSONL transaction files with enforced schema
- Adds ingestion metadata (`ingestion_timestamp`, `source_file`, `ingestion_date`)
- Partitioned by `ingestion_date` for efficient incremental reads
- Data quality checks on null event IDs and invalid amounts

### Silver Layer — Cleansed & Enriched
- **Deduplication** on `event_id` (keeps earliest ingestion)
- **Timestamp parsing** with ISO 8601 fallback handling
- **Quality filters**: non-null IDs, positive amounts, valid status
- **Enrichment**: `event_date`, `event_hour`, `day_of_week`, `amount_category`, `is_cross_border`
- Partitioned by `event_date`

### Anomaly Detection — Risk Scoring Engine
Five rule-based anomaly detectors with weighted composite scoring:

| Rule | Weight | Description |
|------|--------|-------------|
| High Amount (Z-Score) | 35% | Transaction amount z-score > 2 vs customer history |
| Velocity Check | 25% | > 5 transactions per customer in 1-hour window |
| Cross-Border | 15% | Transaction origin outside India |
| Off-Hours | 10% | Transaction between midnight and 5 AM |
| Merchant Risk | 15% | Merchant failure rate > 30% |

Risk levels: **CRITICAL** (≥60), **HIGH** (≥35), **MEDIUM** (≥15), **LOW** (<15)

### Gold Layer — Aggregated Views
Five pre-computed tables for dashboards:
1. **Daily Summary** — transactions, amounts, success rates, cross-border counts
2. **Merchant Risk Profile** — risk scores, failure rates, risk tiers
3. **Customer Risk Profile** — spending, risk flags, merchant diversity
4. **Hourly Pattern** — transaction volume and failure rates by hour
5. **Payment Method Summary** — volume, amounts, success rates by method

## Local Workflows

### Generate Synthetic Events

```bash
python producer/generate_events.py
```
Creates 1,020 events (1,000 unique + 20 replays for dedup testing) in `data/generated/transactions.jsonl`.

### Incremental Batch Ingestion

1. **Generate** a timestamped batch:
   ```bash
   python producer/generate_batch.py
   ```

2. **Run** incremental ingestion:
   ```bash
   python scripts/incremental_ingestion.py
   ```

### How Batch Ingestion Works
1. **Discover** — Find batch files not listed in the checkpoint
2. **Validate** — Check JSON syntax, required fields, positive amounts
3. **Write to Bronze** — Atomic write (temp file → replace) per batch
4. **Checkpoint** — Record processed filenames in `data/processed_batches.json`

### Web UI Dashboard (Localhost)

Run the interactive local dashboard:

```bash
python app.py
```
Then open **`http://localhost:5050`** in your browser. (You can also specify a custom port: `python app.py 8080`)

**UI Features:**
- **Overview Dashboard:** Live KPI metrics, risk distributions, payment methods, status breakups, hourly trends, and critical alerts.
- **Transaction Explorer:** Filter by status, risk level, payment method, or search customer/merchant ID with full pagination.
- **Anomalies View:** In-depth breakdown of the 5 fraud detection rules (Z-Score, Velocity, Cross-Border, Off-Hours, Merchant Failure).
- **Merchant Risk & Customer Risk:** Ranked risk profiles, tier distributions, and failure rates.
- **Interactive Pipeline Control:** Trigger event generation, batch creation, incremental ingestion, and test suites with 1-click live terminal output in the browser.

### Run Tests

```bash
pip install -r requirements.txt
python -m pytest -v
```

## AWS Kinesis Streaming

Stream events to AWS Kinesis Data Streams for real-time processing:

```bash
python producer/kinesis_producer.py \
    --stream-name fraudpulse-transactions \
    --region ap-south-1 \
    --count 1000 \
    --batch-size 500
```

**Features:**
- Batch PutRecords API (up to 500 records per call)
- Exponential backoff on throttling
- Automatic retry for failed records
- Partition by `customer_id` for ordered processing

**Prerequisites:**
- AWS credentials configured (`aws configure` or IAM role)
- Kinesis stream created: `aws kinesis create-stream --stream-name fraudpulse-transactions --shard-count 2 --region ap-south-1`

## Databricks SQL Analytics

The `sql/risk_analytics.sql` file contains 10 ready-to-run queries:
1. Daily Fraud Dashboard Summary
2. Top 10 Highest-Risk Merchants
3. Top 10 Highest-Risk Customers
4. Risk Level Distribution
5. Hourly Transaction Heatmap
6. Payment Method Risk Analysis
7. Cross-Border Transaction Risk
8. Recent Critical Alerts (Last 24h)
9. Customer Velocity Anomalies
10. Merchant Failure Rate Monitoring

## Reliability Considerations

- Previously checkpointed batch files are skipped on subsequent runs
- Each batch is written to a temporary file before replacing its Bronze output file
- All newly discovered batches are validated before the ingestion process begins writing outputs
- The checkpoint is saved after each batch output is written
- Kinesis producer retries failed records with exponential backoff
- This implementation is designed for learning and is not a production-grade exactly-once pipeline
