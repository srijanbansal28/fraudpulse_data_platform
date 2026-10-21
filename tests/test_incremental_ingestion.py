
import json

from scripts import incremental_ingestion as ingestion


def write_batch(batch_dir, filename, records):
    batch_file = batch_dir / filename
    with batch_file.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record) + "\n")
    return batch_file


def test_ingests_new_batch_once(tmp_path, monkeypatch):
    batch_dir = tmp_path / "batches"
    bronze_dir = tmp_path / "bronze"
    checkpoint_file = tmp_path / "processed_batches.json"

    batch_dir.mkdir()

    monkeypatch.setattr(ingestion, "BATCH_DIR", batch_dir)
    monkeypatch.setattr(ingestion, "BRONZE_DIR", bronze_dir)
    monkeypatch.setattr(ingestion, "CHECKPOINT_FILE", checkpoint_file)

    write_batch(
        batch_dir,
        "transactions_test.jsonl",
        [
            {"event_id": "event-1", "amount": 100},
            {"event_id": "event-2", "amount": 200},
        ],
    )

    assert ingestion.ingest_new_batches() == 2

    bronze_file = bronze_dir / "transactions.jsonl"
    assert len(bronze_file.read_text().splitlines()) == 2

    # Rerunning should not append the same batch again.
    assert ingestion.ingest_new_batches() == 0
    assert len(bronze_file.read_text().splitlines()) == 2

    # A new batch should be ingested on the next run.
    write_batch(
        batch_dir,
        "transactions_test_2.jsonl",
        [{"event_id": "event-3", "amount": 300}],
    )

    assert ingestion.ingest_new_batches() == 1
    assert len(bronze_file.read_text().splitlines()) == 3

import pytest


def test_malformed_batch_is_rejected_before_bronze_write(
    tmp_path, monkeypatch
):
    batch_dir = tmp_path / "batches"
    bronze_dir = tmp_path / "bronze"
    checkpoint_file = tmp_path / "processed_batches.json"

    batch_dir.mkdir()

    monkeypatch.setattr(ingestion, "BATCH_DIR", batch_dir)
    monkeypatch.setattr(ingestion, "BRONZE_DIR", bronze_dir)
    monkeypatch.setattr(ingestion, "CHECKPOINT_FILE", checkpoint_file)

    # A valid record followed by malformed JSON.
    batch_file = batch_dir / "transactions_bad.jsonl"
    batch_file.write_text(
        '{"event_id": "event-1", "amount": 100}\n'
        '{"event_id": \n',
        encoding="utf-8",
    )

    # The malformed batch must raise an error.
    with pytest.raises(ValueError, match="Invalid JSON"):
        ingestion.ingest_new_batches()

    # Bronze must not contain partially written records.
    bronze_file = bronze_dir / "transactions.jsonl"
    assert not bronze_file.exists()

    # The failed batch must not be checkpointed.
    assert not checkpoint_file.exists()
