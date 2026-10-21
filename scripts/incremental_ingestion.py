
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BATCH_DIR = PROJECT_ROOT / "data" / "batches"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
CHECKPOINT_FILE = PROJECT_ROOT / "data" / "processed_batches.json"


def load_checkpoint():
    """Return the set of batch filenames already processed."""
    if not CHECKPOINT_FILE.exists():
        return set()

    with CHECKPOINT_FILE.open("r", encoding="utf-8") as file:
        return set(json.load(file))


def save_checkpoint(processed_files):
    """Persist processed batch filenames."""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with CHECKPOINT_FILE.open("w", encoding="utf-8") as file:
        json.dump(sorted(processed_files), file, indent=2)


def ingest_new_batches():
    """Validate new batches before appending them to Bronze."""
    BATCH_DIR.mkdir(parents=True, exist_ok=True)
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    processed_files = load_checkpoint()
    new_files = sorted(BATCH_DIR.glob("transactions_*.jsonl"))
    new_files = [
        path for path in new_files
        if path.name not in processed_files
    ]

    if not new_files:
        print("No new batches to ingest.")
        return 0

    # Read and validate every new batch before writing anything.
    validated_batches = {}

    for batch_file in new_files:
        records = []

        with batch_file.open("r", encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                if not line.strip():
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"Invalid JSON in {batch_file.name}, "
                        f"line {line_number}: {exc}"
                    ) from exc

                if not isinstance(record, dict):
                    raise ValueError(
                        f"Expected a JSON object in {batch_file.name}, "
                        f"line {line_number}"
                    )

                event_id = record.get("event_id")
                transaction_id = record.get("transaction_id")
                amount = record.get("amount")

                if (
                    not isinstance(event_id, str)
                    or not event_id.strip()
                ):
                    raise ValueError(
                        f"Missing or invalid event_id in "
                        f"{batch_file.name}, line {line_number}"
                    )

                if (
                    not isinstance(transaction_id, str)
                    or not transaction_id.strip()
                ):
                    raise ValueError(
                        f"Missing or invalid transaction_id in "
                        f"{batch_file.name}, line {line_number}"
                    )

                if (
                    isinstance(amount, bool)
                    or not isinstance(amount, (int, float))
                ):
                    raise ValueError(
                        f"Invalid amount in {batch_file.name}, "
                        f"line {line_number}"
                    )

                if amount <= 0:
                    raise ValueError(
                        f"Amount must be positive in "
                        f"{batch_file.name}, line {line_number}"
                    )

                records.append(record)

        if not records:
            raise ValueError(f"Batch {batch_file.name} is empty")

        validated_batches[batch_file.name] = records

    # Append only after every new batch passes validation.
    bronze_file = BRONZE_DIR / "transactions.jsonl"
    total_ingested = 0

    with bronze_file.open("a", encoding="utf-8") as output:
        for batch_file in new_files:
            records = validated_batches[batch_file.name]

            for record in records:
                output.write(json.dumps(record) + "\n")
                total_ingested += 1

            processed_files.add(batch_file.name)

    save_checkpoint(processed_files)

    print(
        f"Ingested {total_ingested} transactions "
        f"from {len(new_files)} new batch(es)."
    )
    return total_ingested


if __name__ == "__main__":
    ingest_new_batches()
