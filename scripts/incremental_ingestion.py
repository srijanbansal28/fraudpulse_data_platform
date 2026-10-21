
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
    """Append new batches to Bronze and skip previously processed files."""
    BATCH_DIR.mkdir(parents=True, exist_ok=True)
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    processed_files = load_checkpoint()
    new_files = sorted(BATCH_DIR.glob("transactions_*.jsonl"))
    new_files = [path for path in new_files if path.name not in processed_files]

    if not new_files:
        print("No new batches to ingest.")
        return 0

    bronze_file = BRONZE_DIR / "transactions.jsonl"
    total_ingested = 0

    # Append records before recording the checkpoint.
    with bronze_file.open("a", encoding="utf-8") as output:
        for batch_file in new_files:
            with batch_file.open("r", encoding="utf-8") as source:
                for line in source:
                    record = json.loads(line)
                    output.write(json.dumps(record) + "\n")
                    total_ingested += 1

            processed_files.add(batch_file.name)

    save_checkpoint(processed_files)
    print(f"Ingested {total_ingested} transactions from {len(new_files)} new batch(es).")
    return total_ingested


if __name__ == "__main__":
    ingest_new_batches()
