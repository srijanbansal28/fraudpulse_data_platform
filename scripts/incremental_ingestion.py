
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
    """Validate and ingest batches into separate Bronze files."""
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

    # Validate every pending batch before writing any output.
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

                if not isinstance(event_id, str) or not event_id.strip():
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

    # Write each batch to its own output file.
    total_ingested = 0

    for batch_file in new_files:
        records = validated_batches[batch_file.name]
        output_file = BRONZE_DIR / batch_file.name
        temp_file = output_file.with_suffix(".tmp")

        # Write a complete temporary file before replacing the output.
        with temp_file.open("w", encoding="utf-8") as output:
            for record in records:
                output.write(json.dumps(record) + "\n")

        # Atomic replacement on the same filesystem.
        temp_file.replace(output_file)

        # Record the batch only after its output file is in place.
        processed_files.add(batch_file.name)
        save_checkpoint(processed_files)

        total_ingested += len(records)

    print(
        f"Ingested {total_ingested} transactions "
        f"from {len(new_files)} new batch(es)."
    )
    return total_ingested


if __name__ == "__main__":
    ingest_new_batches()
