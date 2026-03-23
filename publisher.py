import io
import json
import time
import uuid
import random
import fastavro
from datetime import datetime, timedelta
from google.cloud import pubsub_v1

PROJECT_ID       = "ambati-bank-analytics"
TXN_TOPIC        = "ambati-transactions"
CLICK_TOPIC      = "ambati-clickstream"

publisher        = pubsub_v1.PublisherClient()
txn_topic_path   = publisher.topic_path(PROJECT_ID, TXN_TOPIC)
click_topic_path = publisher.topic_path(PROJECT_ID, CLICK_TOPIC)

# ── Avro schemas (must match Pub/Sub schemas exactly) ────────
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

CLICK_AVRO_SCHEMA = fastavro.parse_schema({
    "type": "record",
    "name": "Clickstream",
    "namespace": "com.ambatibank",
    "fields": [
        {"name": "event_id",               "type": "string"},
        {"name": "customer_id",            "type": "string"},
        {"name": "timestamp",              "type": "string"},
        {"name": "record_created_at",      "type": "string"},
        {"name": "session_id",             "type": ["null","string"], "default": None},
        {"name": "session_start",          "type": ["null","string"], "default": None},
        {"name": "session_duration_sec",   "type": ["null","string"], "default": None},
        {"name": "is_new_session",         "type": ["null","string"], "default": None},
        {"name": "session_page_count",     "type": ["null","string"], "default": None},
        {"name": "channel",                "type": ["null","string"], "default": None},
        {"name": "page",                   "type": ["null","string"], "default": None},
        {"name": "previous_page",          "type": ["null","string"], "default": None},
        {"name": "action",                 "type": ["null","string"], "default": None},
        {"name": "element_clicked",        "type": ["null","string"], "default": None},
        {"name": "feature_used",           "type": ["null","string"], "default": None},
        {"name": "device",                 "type": ["null","string"], "default": None},
        {"name": "device_type",            "type": ["null","string"], "default": None},
        {"name": "os",                     "type": ["null","string"], "default": None},
        {"name": "browser",                "type": ["null","string"], "default": None},
        {"name": "app_version",            "type": ["null","string"], "default": None},
        {"name": "screen_resolution",      "type": ["null","string"], "default": None},
        {"name": "network_type",           "type": ["null","string"], "default": None},
        {"name": "ip_country",             "type": ["null","string"], "default": None},
        {"name": "page_load_time_ms",      "type": ["null","string"], "default": None},
        {"name": "time_on_page_sec",       "type": ["null","string"], "default": None},
        {"name": "scroll_depth_pct",       "type": ["null","string"], "default": None},
        {"name": "click_count",            "type": ["null","string"], "default": None},
        {"name": "error_encountered",      "type": ["null","string"], "default": None},
        {"name": "error_message",          "type": ["null","string"], "default": None},
        {"name": "country",                "type": ["null","string"], "default": None},
        {"name": "language",               "type": ["null","string"], "default": None},
        {"name": "utm_source",             "type": ["null","string"], "default": None},
        {"name": "utm_campaign",           "type": ["null","string"], "default": None},
        {"name": "is_authenticated",       "type": ["null","string"], "default": None},
    ]
})

# ── Reference data ────────────────────────────────────────────
COUNTRIES          = ["US", "UK", "Canada"]
CARD_BRANDS        = ["Mastercard", "Visa", "Amex", "Discover"]
CARD_TYPES         = ["Credit", "Debit", "Prepaid"]
MERCHANT_CATS      = ["Food & Dining", "Retail", "Automotive", "Healthcare",
                      "Entertainment", "Utilities", "Travel", "Home & Garden",
                      "Apparel", "Media & Electronics", "Education", "Sports",
                      "Beauty & Wellness", "Grocery", "Insurance"]
MERCHANT_NAMES     = ["Walmart", "Amazon", "Target", "Costco", "Home Depot",
                      "Walgreens", "CVS", "Starbucks", "McDonald's", "Uber",
                      "Netflix", "Apple Store", "Best Buy", "Shell", "BP",
                      "Tesco", "Sainsbury", "ASDA", "Tim Hortons", "Loblaws"]
MERCHANT_CITIES_US = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
MERCHANT_CITIES_UK = ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"]
MERCHANT_CITIES_CA = ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"]
CURRENCIES         = {"US": "USD", "UK": "GBP", "Canada": "CAD"}
STATUSES           = ["pass"] * 9 + ["fail"]
DECLINE_REASONS    = ["insufficient_funds", "card_expired", "invalid_cvv",
                      "fraud_suspected", "limit_exceeded", None]
TXN_TYPES          = ["purchase", "refund", "withdrawal", "transfer", "payment"]
CHANNELS           = ["in_store", "online", "mobile_app", "atm", "contactless"]
PAGES              = ["dashboard", "transfer", "payments", "cards", "statements",
                      "profile", "loans", "support", "investments", "settings"]
ACTIONS            = ["view", "click", "submit", "scroll", "logout", "search"]
DEVICES            = ["iPhone 14", "iPhone 15", "Samsung Galaxy S23",
                      "MacBook Pro", "MacBook Air", "Windows PC", "iPad Pro"]
OS_LIST            = ["iOS 17", "iOS 16", "Android 13", "macOS Sonoma", "Windows 11"]
BROWSERS           = ["Safari", "Chrome", "Firefox", "Edge"]
NETWORK_TYPES      = ["4G", "5G", "WiFi", "3G"]
AUTH_METHODS       = ["pin", "biometric", "password", "otp", "face_id"]
POS_ENTRY_MODES    = ["chip", "swipe", "contactless", "manual", "online"]
FIRST_NAMES        = ["James","Mary","John","Patricia","Robert","Jennifer",
                      "Liam","Emma","Noah","Olivia","Nikhil","Priya",
                      "Raj","Ananya","Mohammed","Fatima","Harry","Sophie"]
LAST_NAMES         = ["Smith","Johnson","Williams","Brown","Jones","Garcia",
                      "Patel","Ambati","Kumar","Singh","Shah","Ahmed","Khan"]

def random_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

def merchant_city(country):
    if country == "US":   return random.choice(MERCHANT_CITIES_US)
    elif country == "UK": return random.choice(MERCHANT_CITIES_UK)
    else:                 return random.choice(MERCHANT_CITIES_CA)

def generate_transaction_id(customer_id):
    ts  = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    uid = str(uuid.uuid4()).replace("-","")[:8].upper()
    cid = customer_id.replace("CUST-","")
    return f"TXN-{ts}-{cid}-{uid}"

def to_str(v):
    """Convert any value to string or None."""
    if v is None:       return None
    if isinstance(v, bool): return str(v).lower()
    return str(v)

def generate_transaction():
    country     = random.choice(COUNTRIES)
    customer_id = f"CUST-{random.randint(1000,9999)}"
    txn_id      = generate_transaction_id(customer_id)
    status      = random.choice(STATUSES)
    amount      = round(random.uniform(1.0, 8000.0), 2)
    card_brand  = random.choice(CARD_BRANDS)
    pos_mode    = random.choice(POS_ENTRY_MODES)
    channel     = random.choice(CHANNELS)

    return {
        "transaction_id":         txn_id,
        "customer_id":            customer_id,
        "timestamp":              datetime.utcnow().isoformat(),
        "processing_date":        datetime.utcnow().strftime("%Y-%m-%d"),
        "record_created_at":      datetime.utcnow().isoformat(),
        "transaction_type":       to_str(random.choice(TXN_TYPES)),
        "card_brand":             to_str(card_brand),
        "card_type":              to_str(random.choice(CARD_TYPES)),
        "card_last_four":         to_str(random.randint(1000,9999)),
        "card_expiry_month":      to_str(random.randint(1,12)),
        "card_expiry_year":       to_str(random.randint(2025,2030)),
        "card_network":           to_str(card_brand),
        "pos_entry_mode":         to_str(pos_mode),
        "chip_used":              to_str(pos_mode == "chip"),
        "contactless":            to_str(pos_mode == "contactless"),
        "auth_method":            to_str(random.choice(AUTH_METHODS)),
        "merchant_id":            to_str(f"MER-{random.randint(10000,99999)}"),
        "merchant_name":          to_str(random.choice(MERCHANT_NAMES)),
        "merchant_category":      to_str(random.choice(MERCHANT_CATS)),
        "merchant_category_code": to_str(random.randint(1000,9999)),
        "merchant_city":          to_str(merchant_city(country)),
        "merchant_state":         to_str(f"State-{random.randint(1,50)}"),
        "merchant_country":       to_str(country),
        "merchant_zip":           to_str(random.randint(10000,99999)),
        "amount":                 to_str(amount),
        "currency":               to_str(CURRENCIES[country]),
        "amount_usd":             to_str(round(amount * random.uniform(0.75,1.25), 2)),
        "cashback_amount":        to_str(round(random.uniform(0,50),2) if random.random()<0.1 else 0.0),
        "fee_amount":             to_str(round(random.uniform(0,5),2) if random.random()<0.2 else 0.0),
        "tax_amount":             to_str(round(amount*0.08, 2)),
        "status":                 to_str(status),
        "decline_reason":         to_str(random.choice(DECLINE_REASONS) if status=="fail" else None),
        "response_code":          to_str("00" if status=="pass" else str(random.randint(1,99)).zfill(2)),
        "is_international":       to_str(random.choice([True,False])),
        "is_online":              to_str(channel=="online"),
        "channel":                to_str(channel),
        "is_flagged":             to_str(random.random()<0.05),
        "risk_score":             to_str(round(random.uniform(0,100),2)),
        "velocity_flag":          to_str(random.random()<0.03),
        "unusual_location":       to_str(random.random()<0.04),
        "night_transaction":      to_str(datetime.utcnow().hour<6 or datetime.utcnow().hour>22),
        "source_system":          to_str("core_banking"),
        "country":                to_str(country),
        "bank_branch_id":         to_str(f"BR-{random.randint(100,999)}"),
        "terminal_id":            to_str(f"TRM-{random.randint(10000,99999)}"),
        "acquirer_id":            to_str(f"ACQ-{random.randint(1000,9999)}"),
        "batch_number":           to_str(random.randint(1,999)),
        "sequence_number":        to_str(random.randint(1,9999)),
    }

def generate_clickstream():
    country     = random.choice(COUNTRIES)
    customer_id = f"CUST-{random.randint(1000,9999)}"
    device      = random.choice(DEVICES)

    return {
        "event_id":             str(uuid.uuid4()),
        "customer_id":          customer_id,
        "timestamp":            datetime.utcnow().isoformat(),
        "record_created_at":    datetime.utcnow().isoformat(),
        "session_id":           to_str(str(uuid.uuid4())),
        "session_start":        to_str((datetime.utcnow()-timedelta(minutes=random.randint(1,60))).isoformat()),
        "session_duration_sec": to_str(random.randint(10,3600)),
        "is_new_session":       to_str(random.choice([True,False])),
        "session_page_count":   to_str(random.randint(1,20)),
        "channel":              to_str(random.choice(["Mobile","Web","Tablet"])),
        "page":                 to_str(random.choice(PAGES)),
        "previous_page":        to_str(random.choice(PAGES)),
        "action":               to_str(random.choice(ACTIONS)),
        "element_clicked":      to_str(random.choice(["button","link","menu","card","tab"])),
        "feature_used":         to_str(random.choice(["transfer","pay_bill","view_statement","apply_loan"])),
        "device":               to_str(device),
        "device_type":          to_str("Mobile" if any(x in device for x in ["iPhone","Samsung","Pixel"]) else "Desktop"),
        "os":                   to_str(random.choice(OS_LIST)),
        "browser":              to_str(random.choice(BROWSERS)),
        "app_version":          to_str(f"{random.randint(1,5)}.{random.randint(0,9)}.{random.randint(0,9)}"),
        "screen_resolution":    to_str(random.choice(["390x844","1920x1080","1440x900","375x667"])),
        "network_type":         to_str(random.choice(NETWORK_TYPES)),
        "ip_country":           to_str(country),
        "page_load_time_ms":    to_str(random.randint(200,5000)),
        "time_on_page_sec":     to_str(random.randint(1,300)),
        "scroll_depth_pct":     to_str(random.randint(0,100)),
        "click_count":          to_str(random.randint(1,30)),
        "error_encountered":    to_str(random.choice([True,False,False,False,False])),
        "error_message":        to_str(random.choice(["timeout","404","500",None,None,None])),
        "country":              to_str(country),
        "language":             to_str(random.choice(["en-US","en-GB","fr-CA","en-CA"])),
        "utm_source":           to_str(random.choice(["organic","email","push","sms",None])),
        "utm_campaign":         to_str(random.choice(["retention","onboarding","promo",None])),
        "is_authenticated":     to_str(random.choice([True,True,True,False])),
    }

def publish_avro(topic_path, data: dict, avro_schema):
    """Encode as Avro binary and publish to Pub/Sub."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, avro_schema, data)
    future = publisher.publish(topic_path, buf.getvalue())
    return future.result()

# ── Main loop ─────────────────────────────────────────────────
print("=" * 65)
print("  Ambati Bank — Real-time Event Publisher")
print(f"  Project  : {PROJECT_ID}")
print(f"  Encoding : Avro Binary")
print(f"  Topics   : {TXN_TOPIC}, {CLICK_TOPIC}")
print("  Rate     : 1 transaction + 1 clickstream per second")
print("  Press Ctrl+C to stop")
print("=" * 65)

txn_count   = 0
click_count = 0

try:
    while True:
        txn = generate_transaction()
        publish_avro(txn_topic_path, txn, TXN_AVRO_SCHEMA)
        txn_count += 1

        click = generate_clickstream()
        publish_avro(click_topic_path, click, CLICK_AVRO_SCHEMA)
        click_count += 1

        print(
            f"[{datetime.utcnow().strftime('%H:%M:%S')}] "
            f"#{txn_count:04d} | "
            f"{txn['country']:<6} | "
            f"{txn['card_brand']:<12} | "
            f"{CURRENCIES[txn['country']]} {txn['amount']:>8} | "
            f"{txn['status'].upper():<4} | "
            f"{txn['merchant_category']:<22} | "
            f"{txn['transaction_id']}"
        )

        time.sleep(1)

except KeyboardInterrupt:
    print("\n" + "=" * 65)
    print("  Publisher stopped.")
    print(f"  Transactions : {txn_count}")
    print(f"  Clickstream  : {click_count}")
    print(f"  Total        : {txn_count + click_count}")
    print("=" * 65)
