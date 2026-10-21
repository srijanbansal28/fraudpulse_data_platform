
import json

from producer.generate_batch import generate_batch


def test_generate_batch_creates_100_transactions(tmp_path, monkeypatch):
    # Redirect output to a temporary test directory.
    monkeypatch.setattr(
        "producer.generate_batch.BATCH_DIR",
        tmp_path,
    )

    generate_batch(batch_size=100)

    # Confirm that one batch file was created.
    files = list(tmp_path.glob("transactions_*.jsonl"))
    assert len(files) == 1

    # Confirm that it contains exactly 100 transactions.
    lines = files[0].read_text(encoding="utf-8").splitlines()
    assert len(lines) == 100

    # Confirm that each line is valid JSON.
    records = [json.loads(line) for line in lines]
    assert all(isinstance(record, dict) for record in records)

    # Confirm that important transaction fields exist.
    required_fields = {
        "event_id",
        "transaction_id",
        "customer_id",
        "merchant_id",
        "event_time",
        "amount",
        "currency",
        "status",
    }

    assert all(required_fields.issubset(record) for record in records)
