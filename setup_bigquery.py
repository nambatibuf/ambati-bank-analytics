from google.cloud import bigquery

PROJECT  = "ambati-bank-analytics"
DATASET  = "ambati_ops"
client   = bigquery.Client(project=PROJECT)

# ── Create dataset ────────────────────────────────────────────
dataset_ref = f"{PROJECT}.{DATASET}"
dataset     = bigquery.Dataset(dataset_ref)
dataset.location = "US"
client.create_dataset(dataset, exists_ok=True)
print(f"Dataset ready : {DATASET}\n")

# ── Schema rules ──────────────────────────────────────────────
# TIMESTAMP / DATE  → keep correct type (needed for partitioning)
# ID / key fields   → STRING REQUIRED (never null)
# everything else   → STRING NULLABLE (dbt casts to correct type)

tables = [
    {
        "name":            "transactions_raw",
        "partition_field": "timestamp",
        "partition_type":  bigquery.TimePartitioningType.DAY,
        "cluster_fields":  ["country", "card_brand", "status"],
        "schema": [
            # Identity — strict
            bigquery.SchemaField("transaction_id",         "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("customer_id",            "STRING",    mode="REQUIRED"),
            # Timestamps — typed for partitioning
            bigquery.SchemaField("timestamp",              "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("processing_date",        "DATE"),
            bigquery.SchemaField("record_created_at",      "TIMESTAMP"),
            # Everything else — STRING, dbt casts later
            bigquery.SchemaField("transaction_type",       "STRING"),
            bigquery.SchemaField("card_brand",             "STRING"),
            bigquery.SchemaField("card_type",              "STRING"),
            bigquery.SchemaField("card_last_four",         "STRING"),
            bigquery.SchemaField("card_expiry_month",      "STRING"),
            bigquery.SchemaField("card_expiry_year",       "STRING"),
            bigquery.SchemaField("card_network",           "STRING"),
            bigquery.SchemaField("pos_entry_mode",         "STRING"),
            bigquery.SchemaField("chip_used",              "STRING"),
            bigquery.SchemaField("contactless",            "STRING"),
            bigquery.SchemaField("auth_method",            "STRING"),
            bigquery.SchemaField("merchant_id",            "STRING"),
            bigquery.SchemaField("merchant_name",          "STRING"),
            bigquery.SchemaField("merchant_category",      "STRING"),
            bigquery.SchemaField("merchant_category_code", "STRING"),
            bigquery.SchemaField("merchant_city",          "STRING"),
            bigquery.SchemaField("merchant_state",         "STRING"),
            bigquery.SchemaField("merchant_country",       "STRING"),
            bigquery.SchemaField("merchant_zip",           "STRING"),
            bigquery.SchemaField("amount",                 "STRING"),
            bigquery.SchemaField("currency",               "STRING"),
            bigquery.SchemaField("amount_usd",             "STRING"),
            bigquery.SchemaField("cashback_amount",        "STRING"),
            bigquery.SchemaField("fee_amount",             "STRING"),
            bigquery.SchemaField("tax_amount",             "STRING"),
            bigquery.SchemaField("status",                 "STRING"),
            bigquery.SchemaField("decline_reason",         "STRING"),
            bigquery.SchemaField("response_code",          "STRING"),
            bigquery.SchemaField("is_international",       "STRING"),
            bigquery.SchemaField("is_online",              "STRING"),
            bigquery.SchemaField("channel",                "STRING"),
            bigquery.SchemaField("is_flagged",             "STRING"),
            bigquery.SchemaField("risk_score",             "STRING"),
            bigquery.SchemaField("velocity_flag",          "STRING"),
            bigquery.SchemaField("unusual_location",       "STRING"),
            bigquery.SchemaField("night_transaction",      "STRING"),
            bigquery.SchemaField("source_system",          "STRING"),
            bigquery.SchemaField("country",                "STRING"),
            bigquery.SchemaField("bank_branch_id",         "STRING"),
            bigquery.SchemaField("terminal_id",            "STRING"),
            bigquery.SchemaField("acquirer_id",            "STRING"),
            bigquery.SchemaField("batch_number",           "STRING"),
            bigquery.SchemaField("sequence_number",        "STRING"),
        ],
    },

    {
        "name":            "clickstream_raw",
        "partition_field": "timestamp",
        "partition_type":  bigquery.TimePartitioningType.DAY,
        "cluster_fields":  ["country", "channel", "device_type"],
        "schema": [
            # Identity — strict
            bigquery.SchemaField("event_id",               "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("customer_id",            "STRING",    mode="REQUIRED"),
            # Timestamps — typed
            bigquery.SchemaField("timestamp",              "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("record_created_at",      "TIMESTAMP"),
            # Everything else — STRING
            bigquery.SchemaField("session_id",             "STRING"),
            bigquery.SchemaField("session_start",          "STRING"),
            bigquery.SchemaField("session_duration_sec",   "STRING"),
            bigquery.SchemaField("is_new_session",         "STRING"),
            bigquery.SchemaField("session_page_count",     "STRING"),
            bigquery.SchemaField("channel",                "STRING"),
            bigquery.SchemaField("page",                   "STRING"),
            bigquery.SchemaField("previous_page",          "STRING"),
            bigquery.SchemaField("action",                 "STRING"),
            bigquery.SchemaField("element_clicked",        "STRING"),
            bigquery.SchemaField("feature_used",           "STRING"),
            bigquery.SchemaField("device",                 "STRING"),
            bigquery.SchemaField("device_type",            "STRING"),
            bigquery.SchemaField("os",                     "STRING"),
            bigquery.SchemaField("browser",                "STRING"),
            bigquery.SchemaField("app_version",            "STRING"),
            bigquery.SchemaField("screen_resolution",      "STRING"),
            bigquery.SchemaField("network_type",           "STRING"),
            bigquery.SchemaField("ip_country",             "STRING"),
            bigquery.SchemaField("page_load_time_ms",      "STRING"),
            bigquery.SchemaField("time_on_page_sec",       "STRING"),
            bigquery.SchemaField("scroll_depth_pct",       "STRING"),
            bigquery.SchemaField("click_count",            "STRING"),
            bigquery.SchemaField("error_encountered",      "STRING"),
            bigquery.SchemaField("error_message",          "STRING"),
            bigquery.SchemaField("country",                "STRING"),
            bigquery.SchemaField("language",               "STRING"),
            bigquery.SchemaField("utm_source",             "STRING"),
            bigquery.SchemaField("utm_campaign",           "STRING"),
            bigquery.SchemaField("is_authenticated",       "STRING"),
        ],
    },

    {
        "name":            "customers_raw",
        "partition_field": "batch_date",
        "partition_type":  bigquery.TimePartitioningType.DAY,
        "cluster_fields":  ["country", "account_status"],
        "schema": [
            # Identity — strict
            bigquery.SchemaField("customer_id",            "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("ssn",                    "STRING",    mode="REQUIRED"),
            # Dates — typed
            bigquery.SchemaField("batch_date",             "DATE",      mode="REQUIRED"),
            bigquery.SchemaField("record_created_at",      "TIMESTAMP"),
            bigquery.SchemaField("record_updated_at",      "TIMESTAMP"),
            # Everything else — STRING
            bigquery.SchemaField("full_name",              "STRING"),
            bigquery.SchemaField("first_name",             "STRING"),
            bigquery.SchemaField("last_name",              "STRING"),
            bigquery.SchemaField("date_of_birth",          "STRING"),
            bigquery.SchemaField("age",                    "STRING"),
            bigquery.SchemaField("gender",                 "STRING"),
            bigquery.SchemaField("nationality",            "STRING"),
            bigquery.SchemaField("marital_status",         "STRING"),
            bigquery.SchemaField("dependents",             "STRING"),
            bigquery.SchemaField("email",                  "STRING"),
            bigquery.SchemaField("phone_primary",          "STRING"),
            bigquery.SchemaField("phone_secondary",        "STRING"),
            bigquery.SchemaField("address_line1",          "STRING"),
            bigquery.SchemaField("address_line2",          "STRING"),
            bigquery.SchemaField("city",                   "STRING"),
            bigquery.SchemaField("state",                  "STRING"),
            bigquery.SchemaField("zip_code",               "STRING"),
            bigquery.SchemaField("country",                "STRING"),
            bigquery.SchemaField("annual_income",          "STRING"),
            bigquery.SchemaField("income_group",           "STRING"),
            bigquery.SchemaField("employment_status",      "STRING"),
            bigquery.SchemaField("employer_name",          "STRING"),
            bigquery.SchemaField("job_title",              "STRING"),
            bigquery.SchemaField("years_employed",         "STRING"),
            bigquery.SchemaField("account_open_date",      "STRING"),
            bigquery.SchemaField("account_type",           "STRING"),
            bigquery.SchemaField("account_status",         "STRING"),
            bigquery.SchemaField("branch_id",              "STRING"),
            bigquery.SchemaField("relationship_manager",   "STRING"),
            bigquery.SchemaField("kyc_status",             "STRING"),
            bigquery.SchemaField("kyc_verified_date",      "STRING"),
            bigquery.SchemaField("source_file",            "STRING"),
        ],
    },

    {
        "name":            "credit_bureau_raw",
        "partition_field": "batch_date",
        "partition_type":  bigquery.TimePartitioningType.DAY,
        "cluster_fields":  ["risk_category", "credit_score_band"],
        "schema": [
            # Identity — strict
            bigquery.SchemaField("ssn",                    "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("customer_id",            "STRING",    mode="REQUIRED"),
            # Dates — typed
            bigquery.SchemaField("batch_date",             "DATE",      mode="REQUIRED"),
            bigquery.SchemaField("record_created_at",      "TIMESTAMP"),
            # Everything else — STRING
            bigquery.SchemaField("report_date",            "STRING"),
            bigquery.SchemaField("bureau_name",            "STRING"),
            bigquery.SchemaField("credit_score",           "STRING"),
            bigquery.SchemaField("credit_score_band",      "STRING"),
            bigquery.SchemaField("credit_score_change",    "STRING"),
            bigquery.SchemaField("score_model",            "STRING"),
            bigquery.SchemaField("total_debt",             "STRING"),
            bigquery.SchemaField("total_credit_limit",     "STRING"),
            bigquery.SchemaField("credit_utilization_pct", "STRING"),
            bigquery.SchemaField("revolving_debt",         "STRING"),
            bigquery.SchemaField("installment_debt",       "STRING"),
            bigquery.SchemaField("mortgage_debt",          "STRING"),
            bigquery.SchemaField("student_loan_debt",      "STRING"),
            bigquery.SchemaField("auto_loan_debt",         "STRING"),
            bigquery.SchemaField("stated_income",          "STRING"),
            bigquery.SchemaField("verified_income",        "STRING"),
            bigquery.SchemaField("dti_ratio",              "STRING"),
            bigquery.SchemaField("dti_category",           "STRING"),
            bigquery.SchemaField("loan_risk_score",        "STRING"),
            bigquery.SchemaField("risk_category",          "STRING"),
            bigquery.SchemaField("bankruptcy_flag",        "STRING"),
            bigquery.SchemaField("bankruptcy_date",        "STRING"),
            bigquery.SchemaField("foreclosure_flag",       "STRING"),
            bigquery.SchemaField("collections_count",      "STRING"),
            bigquery.SchemaField("delinquency_count",      "STRING"),
            bigquery.SchemaField("late_payments_30d",      "STRING"),
            bigquery.SchemaField("late_payments_60d",      "STRING"),
            bigquery.SchemaField("late_payments_90d",      "STRING"),
            bigquery.SchemaField("open_accounts",          "STRING"),
            bigquery.SchemaField("closed_accounts",        "STRING"),
            bigquery.SchemaField("total_accounts",         "STRING"),
            bigquery.SchemaField("oldest_account_years",   "STRING"),
            bigquery.SchemaField("newest_account_months",  "STRING"),
            bigquery.SchemaField("hard_inquiries_6m",      "STRING"),
            bigquery.SchemaField("hard_inquiries_12m",     "STRING"),
            bigquery.SchemaField("source_file",            "STRING"),
        ],
    },
]

# ── Drop & recreate ───────────────────────────────────────────
for t in tables:
    table_id = f"{PROJECT}.{DATASET}.{t['name']}"

    client.delete_table(table_id, not_found_ok=True)
    print(f"Dropped   : {t['name']}")

    table                  = bigquery.Table(table_id, schema=t["schema"])
    table.time_partitioning = bigquery.TimePartitioning(
        type_  = t["partition_type"],
        field  = t["partition_field"],
    )
    table.clustering_fields = t["cluster_fields"]

    client.create_table(table)
    print(
        f"Created   : {DATASET}.{t['name']}"
        f"  |  partition: {t['partition_field']} (daily)"
        f"  |  cluster: {', '.join(t['cluster_fields'])}"
        f"  |  {len(t['schema'])} cols"
    )
    print()

print("=" * 65)
print("BigQuery setup complete!")
print()
print("  Schema strategy:")
print("  TIMESTAMP/DATE  → typed  (partitioning needs real types)")
print("  ID fields       → STRING REQUIRED (keys, never null)")
print("  Everything else → STRING NULLABLE (dbt casts later)")
print()
print("  Project : ambati-bank-analytics")
print("  Dataset : ambati_ops")
print()
for t in tables:
    print(f"  {t['name']:<25} partition: {t['partition_field']}")
print("=" * 65)
