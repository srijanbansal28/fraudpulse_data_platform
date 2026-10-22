"""FraudPulse Dashboard — Flask Web Application."""

import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, stdev

from flask import Flask, jsonify, render_template, request

# ── Project Paths ──
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
GENERATED_DIR = DATA_DIR / "generated"
BATCH_DIR = DATA_DIR / "batches"
BRONZE_DIR = DATA_DIR / "bronze"
CHECKPOINT_FILE = DATA_DIR / "processed_batches.json"

app = Flask(__name__)


# ═══════════════════════════════════════════════════════════
# DATA LOADING & PROCESSING
# ═══════════════════════════════════════════════════════════


def load_transactions():
    """Load all transactions from the generated JSONL file."""
    path = GENERATED_DIR / "transactions.jsonl"
    if not path.exists():
        return []
    records = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def load_bronze():
    """Load all records from Bronze output files."""
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    records = []
    for fp in sorted(BRONZE_DIR.glob("*.jsonl")):
        with fp.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records


def deduplicate(records):
    """Remove duplicate records by event_id (keep first occurrence)."""
    seen = set()
    result = []
    for r in records:
        eid = r.get("event_id")
        if eid and eid not in seen:
            seen.add(eid)
            result.append(r)
    return result


def enrich(records):
    """Add derived columns to each record (Silver-layer enrichment)."""
    for r in records:
        try:
            ts = datetime.fromisoformat(r.get("event_time", ""))
            r["event_date"] = ts.strftime("%Y-%m-%d")
            r["event_hour"] = ts.hour
            r["day_of_week"] = ts.strftime("%A")
        except Exception:
            r["event_date"] = "unknown"
            r["event_hour"] = -1
            r["day_of_week"] = "unknown"

        amt = r.get("amount", 0)
        if amt > 25000:
            r["amount_category"] = "HIGH"
        elif amt > 5000:
            r["amount_category"] = "MEDIUM"
        else:
            r["amount_category"] = "LOW"

        r["is_cross_border"] = r.get("country", "IN") != "IN"
    return records


def score_anomalies(records):
    """Apply 5-rule anomaly detection and assign risk scores."""
    # Precompute customer stats
    cust_amounts = defaultdict(list)
    for r in records:
        cust_amounts[r.get("customer_id", "")].append(r.get("amount", 0))

    cust_stats = {}
    for cid, amounts in cust_amounts.items():
        avg = mean(amounts)
        sd = stdev(amounts) if len(amounts) > 1 else 0
        cust_stats[cid] = {"avg": avg, "stddev": sd, "count": len(amounts)}

    # Precompute merchant failure rates
    merch_total = Counter()
    merch_failed = Counter()
    for r in records:
        mid = r.get("merchant_id", "")
        merch_total[mid] += 1
        if r.get("status") == "FAILED":
            merch_failed[mid] += 1

    merch_failure_rate = {}
    for mid, total in merch_total.items():
        merch_failure_rate[mid] = merch_failed[mid] / total if total > 0 else 0

    # Score each record
    for r in records:
        cid = r.get("customer_id", "")
        mid = r.get("merchant_id", "")
        amt = r.get("amount", 0)
        stats = cust_stats.get(cid, {"avg": 0, "stddev": 0, "count": 0})

        # Rule 1: High Amount Z-Score
        if stats["stddev"] > 0:
            zscore = round((amt - stats["avg"]) / stats["stddev"], 4)
        else:
            zscore = 0.0
        r["amount_zscore"] = zscore
        r["flag_high_amount"] = zscore > 2.0

        # Rule 2: Velocity (simplified — count per customer)
        r["cust_txn_count"] = stats["count"]
        r["flag_velocity"] = stats["count"] > 5

        # Rule 3: Cross-border
        r["flag_cross_border"] = r.get("is_cross_border", False)

        # Rule 4: Off-hours (midnight–5 AM)
        hour = r.get("event_hour", 12)
        r["flag_off_hours"] = 0 <= hour <= 4

        # Rule 5: Merchant failure rate > 30%
        mfr = merch_failure_rate.get(mid, 0)
        r["merchant_failure_rate"] = round(mfr, 4)
        r["flag_merchant_risk"] = mfr > 0.30

        # Composite score
        score = (
            int(r["flag_high_amount"]) * 35
            + int(r["flag_velocity"]) * 25
            + int(r["flag_cross_border"]) * 15
            + int(r["flag_off_hours"]) * 10
            + int(r["flag_merchant_risk"]) * 15
        )
        r["risk_score"] = score

        if score >= 60:
            r["risk_level"] = "CRITICAL"
        elif score >= 35:
            r["risk_level"] = "HIGH"
        elif score >= 15:
            r["risk_level"] = "MEDIUM"
        else:
            r["risk_level"] = "LOW"

    return records


def compute_aggregates(records):
    """Compute Gold-layer aggregates."""
    agg = {}

    # ── Overall Stats ──
    total = len(records)
    amounts = [r.get("amount", 0) for r in records]
    agg["total_transactions"] = total
    agg["total_amount"] = round(sum(amounts), 2)
    agg["avg_amount"] = round(mean(amounts), 2) if amounts else 0

    status_counts = Counter(r.get("status") for r in records)
    agg["success_count"] = status_counts.get("SUCCESS", 0)
    agg["failed_count"] = status_counts.get("FAILED", 0)
    agg["pending_count"] = status_counts.get("PENDING", 0)
    agg["success_rate"] = (
        round(agg["success_count"] / total * 100, 2) if total else 0
    )

    agg["unique_customers"] = len(set(r.get("customer_id") for r in records))
    agg["unique_merchants"] = len(set(r.get("merchant_id") for r in records))

    # ── Risk Distribution ──
    risk_counts = Counter(r.get("risk_level", "LOW") for r in records)
    agg["risk_distribution"] = {
        level: risk_counts.get(level, 0)
        for level in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    }

    risk_amounts = defaultdict(list)
    for r in records:
        risk_amounts[r.get("risk_level", "LOW")].append(r.get("amount", 0))
    agg["risk_avg_amounts"] = {
        level: round(mean(amts), 2) if amts else 0
        for level, amts in risk_amounts.items()
    }

    # ── Payment Method ──
    pm_stats = defaultdict(lambda: {"count": 0, "total": 0, "success": 0, "failed": 0})
    for r in records:
        pm = r.get("payment_method", "OTHER")
        pm_stats[pm]["count"] += 1
        pm_stats[pm]["total"] += r.get("amount", 0)
        if r.get("status") == "SUCCESS":
            pm_stats[pm]["success"] += 1
        elif r.get("status") == "FAILED":
            pm_stats[pm]["failed"] += 1
    agg["payment_methods"] = dict(pm_stats)

    # ── Hourly Pattern ──
    hourly = defaultdict(lambda: {"count": 0, "total": 0, "failed": 0})
    for r in records:
        h = r.get("event_hour", 0)
        hourly[h]["count"] += 1
        hourly[h]["total"] += r.get("amount", 0)
        if r.get("status") == "FAILED":
            hourly[h]["failed"] += 1
    agg["hourly_pattern"] = dict(sorted(hourly.items()))

    # ── Country ──
    country_stats = defaultdict(lambda: {"count": 0, "total": 0, "high_risk": 0})
    for r in records:
        c = r.get("country", "UNKNOWN")
        country_stats[c]["count"] += 1
        country_stats[c]["total"] += r.get("amount", 0)
        if r.get("risk_level") in ("CRITICAL", "HIGH"):
            country_stats[c]["high_risk"] += 1
    agg["country_stats"] = dict(country_stats)

    # ── Top Risky Merchants ──
    merch_risk = defaultdict(lambda: {"count": 0, "total": 0, "scores": [], "high": 0, "failure_rate": 0})
    for r in records:
        mid = r.get("merchant_id", "")
        merch_risk[mid]["count"] += 1
        merch_risk[mid]["total"] += r.get("amount", 0)
        merch_risk[mid]["scores"].append(r.get("risk_score", 0))
        merch_risk[mid]["failure_rate"] = r.get("merchant_failure_rate", 0)
        if r.get("risk_level") in ("CRITICAL", "HIGH"):
            merch_risk[mid]["high"] += 1

    merchant_list = []
    for mid, data in merch_risk.items():
        merchant_list.append({
            "merchant_id": mid,
            "count": data["count"],
            "total": round(data["total"], 2),
            "avg_risk": round(mean(data["scores"]), 2),
            "max_risk": max(data["scores"]),
            "high_risk_count": data["high"],
            "failure_rate": round(data["failure_rate"] * 100, 2),
        })
    merchant_list.sort(key=lambda x: x["avg_risk"], reverse=True)
    agg["top_merchants"] = merchant_list[:20]

    # ── Top Risky Customers ──
    cust_risk = defaultdict(lambda: {"count": 0, "total": 0, "scores": [], "high": 0, "merchants": set()})
    for r in records:
        cid = r.get("customer_id", "")
        cust_risk[cid]["count"] += 1
        cust_risk[cid]["total"] += r.get("amount", 0)
        cust_risk[cid]["scores"].append(r.get("risk_score", 0))
        cust_risk[cid]["merchants"].add(r.get("merchant_id", ""))
        if r.get("risk_level") in ("CRITICAL", "HIGH"):
            cust_risk[cid]["high"] += 1

    customer_list = []
    for cid, data in cust_risk.items():
        customer_list.append({
            "customer_id": cid,
            "count": data["count"],
            "total_spent": round(data["total"], 2),
            "avg_risk": round(mean(data["scores"]), 2),
            "max_risk": max(data["scores"]),
            "high_risk_count": data["high"],
            "unique_merchants": len(data["merchants"]),
        })
    customer_list.sort(key=lambda x: x["avg_risk"], reverse=True)
    agg["top_customers"] = customer_list[:20]

    return agg


def get_processed_data():
    """Full pipeline: load → dedup → enrich → score → aggregate."""
    raw = load_transactions()
    if not raw:
        return [], {}
    deduped = deduplicate(raw)
    enriched = enrich(deduped)
    scored = score_anomalies(enriched)
    agg = compute_aggregates(scored)
    return scored, agg


# ═══════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════


@app.route("/")
def dashboard():
    records, agg = get_processed_data()
    top_alerts = sorted(
        [r for r in records if r.get("risk_level") in ("CRITICAL", "HIGH")],
        key=lambda x: (-x.get("risk_score", 0), -x.get("amount", 0)),
    )[:10]
    return render_template("dashboard.html", agg=agg, alerts=top_alerts, total_raw=len(load_transactions()))


@app.route("/transactions")
def transactions():
    records, _ = get_processed_data()
    # Filtering
    status = request.args.get("status", "")
    risk = request.args.get("risk", "")
    method = request.args.get("method", "")
    search = request.args.get("search", "").strip().lower()

    filtered = records
    if status:
        filtered = [r for r in filtered if r.get("status") == status]
    if risk:
        filtered = [r for r in filtered if r.get("risk_level") == risk]
    if method:
        filtered = [r for r in filtered if r.get("payment_method") == method]
    if search:
        filtered = [
            r for r in filtered
            if search in r.get("event_id", "").lower()
            or search in r.get("transaction_id", "").lower()
            or search in r.get("customer_id", "").lower()
            or search in r.get("merchant_id", "").lower()
        ]

    # Pagination
    page = int(request.args.get("page", 1))
    per_page = 50
    total_pages = max(1, (len(filtered) + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    paginated = filtered[start : start + per_page]

    return render_template(
        "transactions.html",
        records=paginated,
        page=page,
        total_pages=total_pages,
        total=len(filtered),
        filters={"status": status, "risk": risk, "method": method, "search": search},
    )


@app.route("/anomalies")
def anomalies():
    records, agg = get_processed_data()
    flagged = [r for r in records if r.get("risk_score", 0) > 0]
    flagged.sort(key=lambda x: (-x.get("risk_score", 0), -x.get("amount", 0)))

    page = int(request.args.get("page", 1))
    per_page = 50
    total_pages = max(1, (len(flagged) + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page

    return render_template(
        "anomalies.html",
        records=flagged[start : start + per_page],
        page=page,
        total_pages=total_pages,
        total=len(flagged),
        risk_dist=agg.get("risk_distribution", {}),
    )


@app.route("/merchants")
def merchants():
    _, agg = get_processed_data()
    return render_template("merchants.html", merchants=agg.get("top_merchants", []))


@app.route("/customers")
def customers():
    _, agg = get_processed_data()
    return render_template("customers.html", customers=agg.get("top_customers", []))


@app.route("/pipeline")
def pipeline():
    # Get current state
    generated_file = GENERATED_DIR / "transactions.jsonl"
    gen_count = 0
    if generated_file.exists():
        with generated_file.open() as f:
            gen_count = sum(1 for _ in f)

    batch_files = sorted(BATCH_DIR.glob("*.jsonl")) if BATCH_DIR.exists() else []
    bronze_files = sorted(BRONZE_DIR.glob("*.jsonl")) if BRONZE_DIR.exists() else []

    checkpoint = set()
    if CHECKPOINT_FILE.exists():
        with CHECKPOINT_FILE.open() as f:
            checkpoint = set(json.load(f))

    return render_template(
        "pipeline.html",
        gen_count=gen_count,
        batch_files=[f.name for f in batch_files],
        bronze_files=[f.name for f in bronze_files],
        checkpoint=sorted(checkpoint),
    )


@app.route("/api/run/<action>", methods=["POST"])
def run_action(action):
    """Execute pipeline steps."""
    try:
        if action == "generate_events":
            result = subprocess.run(
                [sys.executable, "producer/generate_events.py"],
                capture_output=True, text=True, cwd=str(BASE_DIR), timeout=30,
            )
            return jsonify({"ok": result.returncode == 0, "output": result.stdout + result.stderr})

        elif action == "generate_batch":
            result = subprocess.run(
                [sys.executable, "producer/generate_batch.py"],
                capture_output=True, text=True, cwd=str(BASE_DIR), timeout=30,
            )
            return jsonify({"ok": result.returncode == 0, "output": result.stdout + result.stderr})

        elif action == "ingest":
            result = subprocess.run(
                [sys.executable, "scripts/incremental_ingestion.py"],
                capture_output=True, text=True, cwd=str(BASE_DIR), timeout=30,
            )
            return jsonify({"ok": result.returncode == 0, "output": result.stdout + result.stderr})

        elif action == "run_tests":
            result = subprocess.run(
                [sys.executable, "-m", "pytest", "tests/", "-v"],
                capture_output=True, text=True, cwd=str(BASE_DIR), timeout=60,
            )
            return jsonify({"ok": result.returncode == 0, "output": result.stdout + result.stderr})

        return jsonify({"ok": False, "output": f"Unknown action: {action}"})

    except subprocess.TimeoutExpired:
        return jsonify({"ok": False, "output": "Command timed out."})
    except Exception as e:
        return jsonify({"ok": False, "output": str(e)})


@app.route("/api/stats")
def api_stats():
    """JSON endpoint for dashboard stats (for AJAX refresh)."""
    _, agg = get_processed_data()
    return jsonify(agg)


# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        port = int(sys.argv[1])

    print("\n  🚀 FraudPulse Dashboard")
    print("  ───────────────────────")
    print(f"  Open http://localhost:{port} in your browser\n")
    app.run(debug=True, host="0.0.0.0", port=port)
