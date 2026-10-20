import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker

fake = Faker("en_IN")
random.seed(42)
Faker.seed(42)

# Output directory for generated transaction data
OUTPUT_DIR = Path("data/generated")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Synthetic payment data
MERCHANTS = [f"merch_{i:04d}" for i in range(1, 51)]
CUSTOMERS = [f"cust_{i:05d}" for i in range(1, 501)]
DEVICES = [f"device_{i:04d}" for i in range(1, 201)]


def make_transaction(index, now):
    """Generate one synthetic payment transaction."""

    # Approximately 4% of transactions have unusually high amounts
    unusual = random.random() < 0.04

    amount = (
        round(random.uniform(25000, 90000), 2)
        if unusual
        else round(random.uniform(50, 5000), 2)
    )

    # Generate event timestamps within the previous hour
    event_time = now - timedelta(
        seconds=random.randint(0, 3600)
    )

    return {
        "event_id": f"evt_{uuid.uuid4().hex}",
        "transaction_id": f"txn_{index:08d}",
        "customer_id": random.choice(CUSTOMERS),
        "merchant_id": random.choice(MERCHANTS),
        "event_time": event_time.isoformat(),
        "event_type": "PAYMENT",
        "amount": amount,
        "currency": "INR",
        "payment_method": random.choice(
            ["CARD", "UPI", "WALLET"]
        ),
        "country": random.choice(
            ["IN", "IN", "IN", "US"]
        ),
        "device_id": random.choice(DEVICES),
        "status": random.choices(
            ["SUCCESS", "FAILED", "PENDING"],
            weights=[90, 7, 3],
            k=1,
        )[0],
    }


def main(count=1000):
    """Generate transactions and save them as JSON Lines."""

    now = datetime.now(timezone.utc)

    events = [
        make_transaction(i, now)
        for i in range(1, count + 1)
    ]

    # Replay 20 existing events to test deduplication
    replay_count = min(20, len(events))

    events.extend(
        dict(event)
        for event in events[:replay_count]
    )

    output_file = OUTPUT_DIR / "transactions.jsonl"

    with output_file.open("w", encoding="utf-8") as file:
        for event in events:
            file.write(json.dumps(event) + "\n")

    print(f"Created {len(events)} events")
    print(f"Output: {output_file.resolve()}")


if __name__ == "__main__":
    main()
