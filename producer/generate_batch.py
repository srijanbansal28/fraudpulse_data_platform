"""Generate a timestamped batch of synthetic transactions for incremental ingestion."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure the project root is on sys.path so package imports work
# regardless of how this script is invoked.
_PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from producer.generate_events import make_transaction

# Output directory for batch files — monkeypatched by tests.
BATCH_DIR = Path(__file__).resolve().parents[1] / "data" / "batches"


def generate_batch(batch_size=100):
    """Create one JSONL batch file containing *batch_size* transactions."""
    BATCH_DIR.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%dT%H%M%S")

    records = [make_transaction(i, now) for i in range(1, batch_size + 1)]

    output_file = BATCH_DIR / f"transactions_{timestamp}.jsonl"

    with output_file.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    print(f"Generated {batch_size} transactions → {output_file}")
    return output_file


if __name__ == "__main__":
    generate_batch()
