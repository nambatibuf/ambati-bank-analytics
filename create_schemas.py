import json
from google.cloud import pubsub_v1
from google.pubsub_v1.types import Schema

PROJECT = "ambati-bank-analytics"
client  = pubsub_v1.SchemaServiceClient()

# ── Schema design ─────────────────────────────────────────────
# Rules:
#   1. transaction_id / customer_id / event_id → REQUIRED string
#   2. timestamp → REQUIRED string (ISO 8601)
#   3. All other known fields → OPTIONAL string (null or string)
#   4. No extra/unknown fields allowed → goes to dead letter
#   5. No type validation → everything is string

transaction_schema = json.dumps({
    "type": "record",
    "name": "Transaction",
    "namespace": "com.ambatibank",
    "fields": [
        # ── PRIMARY KEYS (required, must always be present) ──
        {"name": "transaction_id",          "type": "string"},
        {"name": "customer_id",             "type": "string"},
        {"name": "timestamp",               "type": "string"},
        {"name": "processing_date",         "type": "string"},
        {"name": "record_created_at",       "type": "string"},
        # ── ALL OTHER FIELDS (optional string, no type check) ──
        {"name": "transaction_type",        "type": ["null", "string"], "default": None},
        {"name": "card_brand",              "type": ["null", "string"], "default": None},
        {"name": "card_type",               "type": ["null", "string"], "default": None},
        {"name": "card_last_four",          "type": ["null", "string"], "default": None},
        {"name": "card_expiry_month",       "type": ["null", "string"], "default": None},
        {"name": "card_expiry_year",        "type": ["null", "string"], "default": None},
        {"name": "card_network",            "type": ["null", "string"], "default": None},
        {"name": "pos_entry_mode",          "type": ["null", "string"], "default": None},
        {"name": "chip_used",               "type": ["null", "string"], "default": None},
        {"name": "contactless",             "type": ["null", "string"], "default": None},
        {"name": "auth_method",             "type": ["null", "string"], "default": None},
        {"name": "merchant_id",             "type": ["null", "string"], "default": None},
        {"name": "merchant_name",           "type": ["null", "string"], "default": None},
        {"name": "merchant_category",       "type": ["null", "string"], "default": None},
        {"name": "merchant_category_code",  "type": ["null", "string"], "default": None},
        {"name": "merchant_city",           "type": ["null", "string"], "default": None},
        {"name": "merchant_state",          "type": ["null", "string"], "default": None},
        {"name": "merchant_country",        "type": ["null", "string"], "default": None},
        {"name": "merchant_zip",            "type": ["null", "string"], "default": None},
        {"name": "amount",                  "type": ["null", "string"], "default": None},
        {"name": "currency",                "type": ["null", "string"], "default": None},
        {"name": "amount_usd",              "type": ["null", "string"], "default": None},
        {"name": "cashback_amount",         "type": ["null", "string"], "default": None},
        {"name": "fee_amount",              "type": ["null", "string"], "default": None},
        {"name": "tax_amount",              "type": ["null", "string"], "default": None},
        {"name": "status",                  "type": ["null", "string"], "default": None},
        {"name": "decline_reason",          "type": ["null", "string"], "default": None},
        {"name": "response_code",           "type": ["null", "string"], "default": None},
        {"name": "is_international",        "type": ["null", "string"], "default": None},
        {"name": "is_online",               "type": ["null", "string"], "default": None},
        {"name": "channel",                 "type": ["null", "string"], "default": None},
        {"name": "is_flagged",              "type": ["null", "string"], "default": None},
        {"name": "risk_score",              "type": ["null", "string"], "default": None},
        {"name": "velocity_flag",           "type": ["null", "string"], "default": None},
        {"name": "unusual_location",        "type": ["null", "string"], "default": None},
        {"name": "night_transaction",       "type": ["null", "string"], "default": None},
        {"name": "source_system",           "type": ["null", "string"], "default": None},
        {"name": "country",                 "type": ["null", "string"], "default": None},
        {"name": "bank_branch_id",          "type": ["null", "string"], "default": None},
        {"name": "terminal_id",             "type": ["null", "string"], "default": None},
        {"name": "acquirer_id",             "type": ["null", "string"], "default": None},
        {"name": "batch_number",            "type": ["null", "string"], "default": None},
        {"name": "sequence_number",         "type": ["null", "string"], "default": None},
    ]
})

clickstream_schema = json.dumps({
    "type": "record",
    "name": "Clickstream",
    "namespace": "com.ambatibank",
    "fields": [
        # ── PRIMARY KEYS (required) ──
        {"name": "event_id",                "type": "string"},
        {"name": "customer_id",             "type": "string"},
        {"name": "timestamp",               "type": "string"},
        {"name": "record_created_at",       "type": "string"},
        # ── ALL OTHER FIELDS (optional string) ──
        {"name": "session_id",              "type": ["null", "string"], "default": None},
        {"name": "session_start",           "type": ["null", "string"], "default": None},
        {"name": "session_duration_sec",    "type": ["null", "string"], "default": None},
        {"name": "is_new_session",          "type": ["null", "string"], "default": None},
        {"name": "session_page_count",      "type": ["null", "string"], "default": None},
        {"name": "channel",                 "type": ["null", "string"], "default": None},
        {"name": "page",                    "type": ["null", "string"], "default": None},
        {"name": "previous_page",           "type": ["null", "string"], "default": None},
        {"name": "action",                  "type": ["null", "string"], "default": None},
        {"name": "element_clicked",         "type": ["null", "string"], "default": None},
        {"name": "feature_used",            "type": ["null", "string"], "default": None},
        {"name": "device",                  "type": ["null", "string"], "default": None},
        {"name": "device_type",             "type": ["null", "string"], "default": None},
        {"name": "os",                      "type": ["null", "string"], "default": None},
        {"name": "browser",                 "type": ["null", "string"], "default": None},
        {"name": "app_version",             "type": ["null", "string"], "default": None},
        {"name": "screen_resolution",       "type": ["null", "string"], "default": None},
        {"name": "network_type",            "type": ["null", "string"], "default": None},
        {"name": "ip_country",              "type": ["null", "string"], "default": None},
        {"name": "page_load_time_ms",       "type": ["null", "string"], "default": None},
        {"name": "time_on_page_sec",        "type": ["null", "string"], "default": None},
        {"name": "scroll_depth_pct",        "type": ["null", "string"], "default": None},
        {"name": "click_count",             "type": ["null", "string"], "default": None},
        {"name": "error_encountered",       "type": ["null", "string"], "default": None},
        {"name": "error_message",           "type": ["null", "string"], "default": None},
        {"name": "country",                 "type": ["null", "string"], "default": None},
        {"name": "language",                "type": ["null", "string"], "default": None},
        {"name": "utm_source",              "type": ["null", "string"], "default": None},
        {"name": "utm_campaign",            "type": ["null", "string"], "default": None},
        {"name": "is_authenticated",        "type": ["null", "string"], "default": None},
    ]
})

print("=" * 65)
print("  Ambati Bank — Recreate Pub/Sub Schemas")
print(f"  Project : {PROJECT}")
print("=" * 65)
print()
print("  Schema rules:")
print("  ✓ transaction_id, customer_id, event_id → REQUIRED")
print("  ✓ timestamp → REQUIRED")
print("  ✓ All other fields → optional string (no type check)")
print("  ✗ Extra/unknown fields → rejected → dead letter quarantine")
print()

schemas = [
    {
        "id":         "ambati-transactions-schema",
        "definition": transaction_schema,
    },
    {
        "id":         "ambati-clickstream-schema",
        "definition": clickstream_schema,
    },
]

for s in schemas:
    schema_path = f"projects/{PROJECT}/schemas/{s['id']}"

    # Delete existing schema first
    try:
        client.delete_schema(request={"name": schema_path})
        print(f"  Deleted old : {s['id']}")
    except Exception:
        pass

    # Create fresh schema
    try:
        client.create_schema(
            request={
                "parent":    f"projects/{PROJECT}",
                "schema":    Schema(
                    name       = schema_path,
                    type_      = Schema.Type.AVRO,
                    definition = s["definition"],
                ),
                "schema_id": s["id"],
            }
        )
        fields = len(json.loads(s["definition"])["fields"])
        print(f"  Created     : {s['id']} ({fields} fields)")
        print()
    except Exception as e:
        print(f"  Error       : {s['id']} -> {e}")
        print()

print("=" * 65)
print("  Done! Now fix publisher to send correct Avro JSON format")
print("  then run wire_pubsub_bigquery.py")
print("=" * 65)
