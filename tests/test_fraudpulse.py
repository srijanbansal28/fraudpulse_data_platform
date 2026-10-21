
def remove_duplicate_events(records):
    """Keep the first record for each non-empty event_id."""
    seen = set()
    result = []

    for record in records:
        event_id = record.get("event_id")

        if not event_id or event_id in seen:
            continue

        seen.add(event_id)
        result.append(record)

    return result


def is_valid_amount(amount):
    """Return True only for positive numeric amounts."""
    try:
        return amount is not None and float(amount) > 0
    except (ValueError, TypeError):
        return False


def test_duplicate_events_are_removed():
    records = [
        {"event_id": "evt-1", "amount": 100},
        {"event_id": "evt-1", "amount": 100},
        {"event_id": "evt-2", "amount": 250},
    ]

    result = remove_duplicate_events(records)

    assert len(result) == 2
    assert {r["event_id"] for r in result} == {"evt-1", "evt-2"}


def test_invalid_amounts_are_rejected():
    assert is_valid_amount(100) is True
    assert is_valid_amount(0) is False
    assert is_valid_amount(-5) is False
    assert is_valid_amount(None) is False
    assert is_valid_amount("not-a-number") is False


def test_missing_event_ids_are_skipped():
    records = [
        {"event_id": "", "amount": 50},
        {"amount": 100},
        {"event_id": "evt-3", "amount": 200},
    ]

    result = remove_duplicate_events(records)

    assert len(result) == 1
    assert result[0]["event_id"] == "evt-3"
