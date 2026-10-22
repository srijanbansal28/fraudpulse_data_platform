"""Send synthetic fraud-detection events to an AWS Kinesis Data Stream."""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure the project root is on sys.path so package imports work
# regardless of how this script is invoked.
_PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import boto3
from botocore.exceptions import ClientError

from producer.generate_events import make_transaction


def put_records_batch(client, stream_name, records, max_retries=5):
    """Send a batch of records to Kinesis with exponential backoff."""
    entries = [
        {
            "Data": json.dumps(record).encode("utf-8"),
            "PartitionKey": record.get("customer_id", "unknown"),
        }
        for record in records
    ]

    attempt = 0
    while attempt < max_retries:
        try:
            response = client.put_records(
                StreamName=stream_name, Records=entries
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] in (
                "ProvisionedThroughputExceededException",
                "KMSThrottlingException",
            ):
                wait = 2 ** attempt
                print(f"  Throttled – retrying in {wait}s (attempt {attempt + 1})")
                time.sleep(wait)
                attempt += 1
                continue
            raise

        failed = response.get("FailedRecordCount", 0)
        if failed == 0:
            return len(entries), 0

        # Retry only the failed records.
        retry_entries = [
            entry
            for entry, result in zip(entries, response["Records"])
            if "ErrorCode" in result
        ]
        entries = retry_entries
        wait = 2 ** attempt
        print(f"  {failed} record(s) failed – retrying in {wait}s")
        time.sleep(wait)
        attempt += 1

    return len(records) - len(entries), len(entries)


def stream_events(stream_name, region, total_events, batch_size=500):
    """Generate and send events to a Kinesis stream in batches."""
    client = boto3.client("kinesis", region_name=region)
    now = datetime.now(timezone.utc)

    sent_total = 0
    failed_total = 0

    for offset in range(0, total_events, batch_size):
        chunk_size = min(batch_size, total_events - offset)
        records = [
            make_transaction(offset + i, now) for i in range(1, chunk_size + 1)
        ]

        sent, failed = put_records_batch(client, stream_name, records)
        sent_total += sent
        failed_total += failed

        print(
            f"Batch {offset // batch_size + 1}: "
            f"sent={sent}, failed={failed}, "
            f"cumulative={sent_total}/{total_events}"
        )

    print(f"\nDone. Sent: {sent_total}  Failed: {failed_total}")
    return sent_total, failed_total


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stream synthetic transactions to AWS Kinesis."
    )
    parser.add_argument(
        "--stream-name",
        default="fraudpulse-transactions",
        help="Kinesis stream name (default: fraudpulse-transactions)",
    )
    parser.add_argument(
        "--region",
        default="ap-south-1",
        help="AWS region (default: ap-south-1)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1000,
        help="Number of events to generate (default: 1000)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Records per PutRecords call (default: 500, max: 500)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    stream_events(
        stream_name=args.stream_name,
        region=args.region,
        total_events=args.count,
        batch_size=min(args.batch_size, 500),
    )
