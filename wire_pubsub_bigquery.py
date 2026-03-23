from google.cloud import pubsub_v1, storage

PROJECT     = "ambati-bank-analytics"
BUCKET_NAME = "ambati-raw-landing"
publisher   = pubsub_v1.PublisherClient()
subscriber  = pubsub_v1.SubscriberClient()
gcs_client  = storage.Client(project=PROJECT)

print("=" * 65)
print("  Ambati Bank — Wire Pub/Sub → BigQuery + Dead Letter")
print(f"  Project  : {PROJECT}")
print(f"  Encoding : Avro Binary")
print("=" * 65)
print()

# ── Step 1: Recreate topics with BINARY schema ───────────────
print("  Step 1: Recreating topics with Avro Binary encoding...")
print()

topic_schemas = [
    {
        "topic":     "ambati-transactions",
        "schema_id": "ambati-transactions-schema",
    },
    {
        "topic":     "ambati-clickstream",
        "schema_id": "ambati-clickstream-schema",
    },
]

for t in topic_schemas:
    topic_path  = f"projects/{PROJECT}/topics/{t['topic']}"
    schema_path = f"projects/{PROJECT}/schemas/{t['schema_id']}"

    # Delete all subscriptions first
    try:
        subs = list(publisher.list_topic_subscriptions(
            request={"topic": topic_path}
        ))
        for sub in subs:
            subscriber.delete_subscription(request={"subscription": sub})
            print(f"  Deleted sub : {sub.split('/')[-1]}")
    except Exception:
        pass

    # Delete topic
    try:
        publisher.delete_topic(request={"topic": topic_path})
        print(f"  Deleted     : {t['topic']}")
    except Exception:
        pass

    # Recreate with BINARY Avro encoding
    try:
        publisher.create_topic(request={
            "name": topic_path,
            "schema_settings": {
                "schema":   schema_path,
                "encoding": 2,  # 2 = BINARY encoding
            }
        })
        print(f"  Created     : {t['topic']} (Avro Binary + schema)")
        print()
    except Exception as e:
        print(f"  Error       : {t['topic']} -> {e}")
        print()

# ── Step 2: Dead Letter topics ───────────────────────────────
print("  Step 2: Creating dead letter topics...")
print()

for topic_id in ["ambati-transactions-dead-letter", "ambati-clickstream-dead-letter"]:
    topic_path = f"projects/{PROJECT}/topics/{topic_id}"
    try:
        publisher.create_topic(request={"name": topic_path})
        print(f"  Created DL  : {topic_id}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  Exists DL   : {topic_id}")
        else:
            print(f"  Error DL    : {e}")
print()

# ── Step 3: Dead Letter → Cloud Storage ─────────────────────
print("  Step 3: Wiring dead letter → Cloud Storage...")
print()

dl_subscriptions = [
    {
        "topic":  "ambati-transactions-dead-letter",
        "sub":    "ambati-transactions-dl-gcs-sub",
        "folder": "dead-letter/transactions/",
    },
    {
        "topic":  "ambati-clickstream-dead-letter",
        "sub":    "ambati-clickstream-dl-gcs-sub",
        "folder": "dead-letter/clickstream/",
    },
]

for s in dl_subscriptions:
    topic_path = f"projects/{PROJECT}/topics/{s['topic']}"
    sub_path   = f"projects/{PROJECT}/subscriptions/{s['sub']}"
    try:
        subscriber.delete_subscription(request={"subscription": sub_path})
    except Exception:
        pass
    try:
        subscriber.create_subscription(request={
            "name":  sub_path,
            "topic": topic_path,
            "cloud_storage_config": {
                "bucket":          BUCKET_NAME,
                "filename_prefix": s["folder"],
                "filename_suffix": ".avro",
                "max_duration":    {"seconds": 300},
            }
        })
        print(f"  Wired : {s['topic']}")
        print(f"       → gs://{BUCKET_NAME}/{s['folder']}")
    except Exception as e:
        print(f"  Error : {e}")
    print()

# ── Step 4: BigQuery subscriptions ──────────────────────────
print("  Step 4: Creating BigQuery subscriptions...")
print()

dl_prefix = f"projects/{PROJECT}/topics"

subscriptions = [
    {
        "topic":    "ambati-transactions",
        "sub":      "ambati-transactions-bq-sub",
        "table":    "ambati-bank-analytics.ambati_ops.transactions_raw",
        "dl_topic": "ambati-transactions-dead-letter",
    },
    {
        "topic":    "ambati-clickstream",
        "sub":      "ambati-clickstream-bq-sub",
        "table":    "ambati-bank-analytics.ambati_ops.clickstream_raw",
        "dl_topic": "ambati-clickstream-dead-letter",
    },
]

for s in subscriptions:
    topic_path = f"projects/{PROJECT}/topics/{s['topic']}"
    sub_path   = f"projects/{PROJECT}/subscriptions/{s['sub']}"
    dl_path    = f"{dl_prefix}/{s['dl_topic']}"

    try:
        subscriber.delete_subscription(request={"subscription": sub_path})
        print(f"  Deleted old : {s['sub']}")
    except Exception:
        pass

    try:
        subscriber.create_subscription(request={
            "name":  sub_path,
            "topic": topic_path,
            "bigquery_config": {
                "table":               s["table"],
                "use_topic_schema":    True,
                "write_metadata":      False,
                "drop_unknown_fields": True,
            },
            "dead_letter_policy": {
                "dead_letter_topic":     dl_path,
                "max_delivery_attempts": 5,
            },
            "retry_policy": {
                "minimum_backoff": {"seconds": 10},
                "maximum_backoff": {"seconds": 60},
            }
        })
        print(f"  Created  : {s['sub']}")
        print(f"  Topic    : {s['topic']}")
        print(f"  Table    : {s['table']}")
        print(f"  Dead LTR : gs://{BUCKET_NAME}/dead-letter/")
        print()
    except Exception as e:
        print(f"  Error    : {s['sub']} -> {e}")
        print()

print("=" * 65)
print("  Pipeline wired with Avro Binary encoding!")
print()
print("  Happy path:")
print("  publisher.py → Pub/Sub (Avro Binary) → BigQuery")
print()
print("  Failed messages (5 retries):")
print(f"  → Dead Letter → gs://{BUCKET_NAME}/dead-letter/")
print()
print("  Next: install fastavro then run publisher.py")
print("  pip install fastavro")
print("=" * 65)
