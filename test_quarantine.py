import io
import json
import fastavro
from google.cloud import pubsub_v1
from datetime import datetime

PROJECT_ID     = "ambati-bank-analytics"
TXN_TOPIC      = "ambati-transactions"
publisher      = pubsub_v1.PublisherClient()
txn_topic_path = publisher.topic_path(PROJECT_ID, TXN_TOPIC)

# ── Same Avro schema as publisher ────────────────────────────
TXN_AVRO_SCHEMA = fastavro.parse_schema({
    "type": "record",
    "name": "Transaction",
    "namespace": "com.ambatibank",
    "fields": [
        {"name": "transaction_id",         "type": "string"},
        {"name": "customer_id",            "type": "string"},
        {"name": "timestamp",              "type": "string"},
        {"name": "processing_date",        "type": "string"},
        {"name": "record_created_at",      "type": "string"},
        {"name": "transaction_type",       "type": ["null","string"], "default": None},
        {"name": "card_brand",             "type": ["null","string"], "default": None},
        {"name": "card_type",              "type": ["null","string"], "default": None},
        {"name": "card_last_four",         "type": ["null","string"], "default": None},
        {"name": "card_expiry_month",      "type": ["null","string"], "default": None},
        {"name": "card_expiry_year",       "type": ["null","string"], "default": None},
        {"name": "card_network",           "type": ["null","string"], "default": None},
        {"name": "pos_entry_mode",         "type": ["null","string"], "default": None},
        {"name": "chip_used",              "type": ["null","string"], "default": None},
        {"name": "contactless",            "type": ["null","string"], "default": None},
        {"name": "auth_method",            "type": ["null","string"], "default": None},
        {"name": "merchant_id",            "type": ["null","string"], "default": None},
        {"name": "merchant_name",          "type": ["null","string"], "default": None},
        {"name": "merchant_category",      "type": ["null","string"], "default": None},
        {"name": "merchant_category_code", "type": ["null","string"], "default": None},
        {"name": "merchant_city",          "type": ["null","string"], "default": None},
        {"name": "merchant_state",         "type": ["null","string"], "default": None},
        {"name": "merchant_country",       "type": ["null","string"], "default": None},
        {"name": "merchant_zip",           "type": ["null","string"], "default": None},
        {"name": "amount",                 "type": ["null","string"], "default": None},
        {"name": "currency",               "type": ["null","string"], "default": None},
        {"name": "amount_usd",             "type": ["null","string"], "default": None},
        {"name": "cashback_amount",        "type": ["null","string"], "default": None},
        {"name": "fee_amount",             "type": ["null","string"], "default": None},
        {"name": "tax_amount",             "type": ["null","string"], "default": None},
        {"name": "status",                 "type": ["null","string"], "default": None},
        {"name": "decline_reason",         "type": ["null","string"], "default": None},
        {"name": "response_code",          "type": ["null","string"], "default": None},
        {"name": "is_international",       "type": ["null","string"], "default": None},
        {"name": "is_online",              "type": ["null","string"], "default": None},
        {"name": "channel",                "type": ["null","string"], "default": None},
        {"name": "is_flagged",             "type": ["null","string"], "default": None},
        {"name": "risk_score",             "type": ["null","string"], "default": None},
        {"name": "velocity_flag",          "type": ["null","string"], "default": None},
        {"name": "unusual_location",       "type": ["null","string"], "default": None},
        {"name": "night_transaction",      "type": ["null","string"], "default": None},
        {"name": "source_system",          "type": ["null","string"], "default": None},
        {"name": "country",                "type": ["null","string"], "default": None},
        {"name": "bank_branch_id",         "type": ["null","string"], "default": None},
        {"name": "terminal_id",            "type": ["null","string"], "default": None},
        {"name": "acquirer_id",            "type": ["null","string"], "default": None},
        {"name": "batch_number",           "type": ["null","string"], "default": None},
        {"name": "sequence_number",        "type": ["null","string"], "default": None},
    ]
})

def publish_avro(data):
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, TXN_AVRO_SCHEMA, data)
    future = publisher.publish(txn_topic_path, buf.getvalue())
    return future.result()

def publish_raw_json(data):
    """Send plain JSON — bypasses fastavro, goes raw to Pub/Sub."""
    message = json.dumps(data).encode("utf-8")
    future  = publisher.publish(txn_topic_path, message)
    return future.result()

print("=" * 65)
print("  Ambati Bank — Quarantine Test")
print("  Sending bad messages to test dead letter routing")
print("=" * 65)
print()

# ── TEST 1: Valid message (should pass) ──────────────────────
print("  Test 1: Valid message (should reach BigQuery)")
print("  ─────────────────────────────────────────────")
valid_msg = {
    "transaction_id":   "TXN-TEST-VALID-001",
    "customer_id":      "CUST-9999",
    "timestamp":        datetime.utcnow().isoformat(),
    "processing_date":  datetime.utcnow().strftime("%Y-%m-%d"),
    "record_created_at":datetime.utcnow().isoformat(),
    "card_brand":       "Visa",
    "amount":           "999.99",
    "status":           "pass",
    "country":          "US",
    "transaction_type": "purchase",
    "merchant_name":    "Test Merchant",
    "merchant_category":"Retail",
    "currency":         "USD",
}
try:
    publish_avro(valid_msg)
    print("  ✅ SENT — transaction_id: TXN-TEST-VALID-001")
    print("     → Should appear in BigQuery transactions_raw")
except Exception as e:
    print(f"  ❌ FAILED: {e}")
print()

# ── TEST 2: Missing primary key (should fail → quarantine) ───
print("  Test 2: Missing transaction_id (should go to quarantine)")
print("  ─────────────────────────────────────────────────────────")
missing_pk = {
    # transaction_id is MISSING — primary key required!
    "customer_id":       "CUST-8888",
    "timestamp":         datetime.utcnow().isoformat(),
    "processing_date":   datetime.utcnow().strftime("%Y-%m-%d"),
    "record_created_at": datetime.utcnow().isoformat(),
    "card_brand":        "Mastercard",
    "amount":            "500.00",
    "status":            "pass",
}
try:
    publish_avro(missing_pk)
    print("  ⚠️  SENT (fastavro may have filled default)")
except Exception as e:
    print(f"  ✅ BLOCKED by fastavro locally: {type(e).__name__}")
    print(f"     → {str(e)[:80]}")
print()

# ── TEST 3: Extra unknown field (should fail → quarantine) ───
print("  Test 3: Extra unknown field (should go to quarantine)")
print("  ──────────────────────────────────────────────────────")
extra_field = {
    "transaction_id":    "TXN-TEST-EXTRA-003",
    "customer_id":       "CUST-7777",
    "timestamp":         datetime.utcnow().isoformat(),
    "processing_date":   datetime.utcnow().strftime("%Y-%m-%d"),
    "record_created_at": datetime.utcnow().isoformat(),
    "card_brand":        "Amex",
    "amount":            "250.00",
    "status":            "pass",
    "UNKNOWN_NEW_FIELD": "this field doesnt exist in schema",  # ← bad field
}
try:
    publish_avro(extra_field)
    print("  ⚠️  SENT — fastavro ignored unknown field")
    print("     → Pub/Sub schema will reject it → dead letter")
except Exception as e:
    print(f"  ✅ BLOCKED by fastavro locally: {type(e).__name__}")
    print(f"     → {str(e)[:80]}")
print()

# ── TEST 4: Completely wrong format (raw JSON, no avro) ──────
print("  Test 4: Raw JSON no Avro encoding (should go to quarantine)")
print("  ─────────────────────────────────────────────────────────────")
raw_json_msg = {
    "transaction_id": "TXN-TEST-RAW-004",
    "customer_id":    "CUST-6666",
    "card_brand":     "Discover",
    "amount":         999.99,  # number not string
}
try:
    publish_raw_json(raw_json_msg)
    print("  ⚠️  SENT as raw JSON")
    print("     → Pub/Sub binary schema will reject → dead letter")
except Exception as e:
    print(f"  ❌ Error: {e}")
print()

print("=" * 65)
print("  Tests complete!")
print()
print("  Now check:")
print("  1. BigQuery → transactions_raw → TXN-TEST-VALID-001 exists")
print("  2. Cloud Storage → ambati-raw-landing/dead-letter/")
print("     → Failed messages should appear here within 1-2 mins")
print("=" * 65)
