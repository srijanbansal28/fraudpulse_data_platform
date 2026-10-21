# fraudpulse_data_platform
Financial transaction anomaly detection pipeline using Python, PySpark, Delta Lake, and Databricks.

## Incremental Batch Ingestion

FraudPulse includes a local Python-based incremental ingestion workflow for processing synthetic financial transaction batches.

### How it works

1. **Generate:** Create timestamped JSONL batches using `producer/generate_batch.py`.
2. **Discover:** Identify batch files that are not listed in the ingestion checkpoint.
3. **Validate:** Check JSON syntax, required transaction identifiers, positive numeric amounts, and non-empty batches.
4. **Write to Bronze:** Write each validated batch to its own output file under `data/bronze/`.
5. **Checkpoint:** Record successfully written batch filenames in `data/processed_batches.json`.
6. **Test:** Use pytest to verify ingestion, validation, reruns, and recovery behavior.

### Run locally

Generate a batch:

```bash
python producer/generate_batch.py
```

Run incremental ingestion:

```bash
python scripts/incremental_ingestion.py
```

Run the test suite:

```bash
python -m pytest -v
```

### Reliability considerations

- Previously checkpointed batch files are skipped on subsequent runs.
- Each batch is written to a temporary file before replacing its Bronze output file.
- All newly discovered batches are validated before the ingestion process begins writing outputs.
- The checkpoint is saved after each batch output is written.
- This implementation is designed for local learning and is not a production-grade exactly-once pipeline. Concurrent ingestion, checkpoint loss, and storage failures require additional handling.
