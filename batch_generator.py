import json
import uuid
import random
import os
from datetime import datetime, date, timedelta
from google.cloud import storage

PROJECT_ID  = "ambati-bank-analytics"
BUCKET_NAME = "ambati-raw-landing"
client      = storage.Client(project=PROJECT_ID)

COUNTRIES   = ["US", "UK", "Canada"]
GENDERS     = ["Male", "Female", "Non-binary"]
MARITAL     = ["Single", "Married", "Divorced", "Widowed"]
EMP_STATUS  = ["Full-time", "Part-time", "Self-employed", "Unemployed", "Retired"]
ACC_TYPES   = ["Checking", "Savings", "Joint", "Business"]
ACC_STATUS  = ["Active", "Dormant", "Closed", "Suspended"]
KYC_STATUS  = ["Verified", "Pending", "Failed"]
INCOME_GRP  = ["Low Income", "Middle Income", "High Income"]
BUREAUS     = ["Equifax", "Experian", "TransUnion"]
SCORE_BANDS = {(300,579):"Poor",(580,669):"Fair",(670,739):"Good",(740,799):"Very Good",(800,850):"Excellent"}
DTI_CATS    = {(0,35):"Low Risk",(36,49):"Manageable",(50,99):"Risky",(100,199):"High Risk",(200,999):"Severe"}
RISK_CATS   = {(35,49):"Low Risk",(50,64):"Moderate",(65,83):"High Risk"}

FIRST_NAMES = ["James","Mary","John","Patricia","Robert","Jennifer","Michael",
               "Linda","William","Barbara","David","Susan","Liam","Emma","Noah",
               "Olivia","Nikhil","Priya","Raj","Ananya","Mohammed","Fatima","Wei"]
LAST_NAMES  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller",
               "Davis","Wilson","Taylor","Patel","Ambati","Kumar","Singh","Shah",
               "Ahmed","Khan","Zhang","Wang","Chen","Murphy","Robertson"]
CITIES_US   = ["New York","Los Angeles","Chicago","Houston","Phoenix","Dallas","Austin"]
CITIES_UK   = ["London","Manchester","Birmingham","Leeds","Glasgow","Liverpool"]
CITIES_CA   = ["Toronto","Vancouver","Montreal","Calgary","Ottawa","Edmonton"]
EMPLOYERS   = ["Google","Amazon","Microsoft","JPMorgan","Goldman Sachs","Deloitte",
               "IBM","Accenture","KPMG","PwC","NHS","Tesco","RBC","TD Bank"]
JOB_TITLES  = ["Software Engineer","Data Analyst","Product Manager","Accountant",
               "Nurse","Teacher","Sales Manager","Operations Lead","Consultant"]

def band(value, ranges):
    for (lo, hi), label in ranges.items():
        if lo <= value <= hi:
            return label
    return "Unknown"

def random_date(start_year=1950, end_year=2000):
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def generate_customers(n=500):
    records = []
    for _ in range(n):
        country    = random.choice(COUNTRIES)
        first      = random.choice(FIRST_NAMES)
        last       = random.choice(LAST_NAMES)
        dob        = random_date(1950, 2000)
        age        = (date.today() - dob).days // 365
        income     = round(random.uniform(15000, 350000), 2)
        cities     = CITIES_US if country=="US" else CITIES_UK if country=="UK" else CITIES_CA
        customer_id= f"CUST-{random.randint(1000,9999)}"

        records.append({
            "customer_id":          customer_id,
            "ssn":                  f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}",
            "full_name":            f"{first} {last}",
            "first_name":           first,
            "last_name":            last,
            "date_of_birth":        dob.isoformat(),
            "age":                  age,
            "gender":               random.choice(GENDERS),
            "nationality":          country,
            "marital_status":       random.choice(MARITAL),
            "dependents":           random.randint(0, 5),
            "email":                f"{first.lower()}.{last.lower()}{random.randint(1,99)}@email.com",
            "phone_primary":        f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            "phone_secondary":      f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            "address_line1":        f"{random.randint(1,9999)} {last} Street",
            "address_line2":        f"Apt {random.randint(1,999)}" if random.random()<0.3 else "",
            "city":                 random.choice(cities),
            "state":                f"State-{random.randint(1,50)}",
            "zip_code":             str(random.randint(10000,99999)),
            "country":              country,
            "annual_income":        income,
            "income_group":         "High Income" if income>150000 else "Middle Income" if income>50000 else "Low Income",
            "employment_status":    random.choice(EMP_STATUS),
            "employer_name":        random.choice(EMPLOYERS),
            "job_title":            random.choice(JOB_TITLES),
            "years_employed":       random.randint(0, 35),
            "account_open_date":    random_date(2000, 2023).isoformat(),
            "account_type":         random.choice(ACC_TYPES),
            "account_status":       random.choice(ACC_STATUS),
            "branch_id":            f"BR-{random.randint(100,999)}",
            "relationship_manager": f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            "kyc_status":           random.choice(KYC_STATUS),
            "kyc_verified_date":    random_date(2020, 2024).isoformat(),
            "record_created_at":    datetime.utcnow().isoformat(),
            "record_updated_at":    datetime.utcnow().isoformat(),
            "batch_date":           date.today().isoformat(),
            "source_file":          f"customers_{date.today().isoformat()}.json",
        })
    return records

def generate_credit_bureau(n=500):
    records = []
    for _ in range(n):
        ssn         = f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}"
        score       = random.randint(300, 850)
        total_debt  = round(random.uniform(0, 600000), 2)
        credit_lim  = round(random.uniform(1000, 100000), 2)
        dti         = round(random.uniform(0, 500), 2)
        risk_score  = random.randint(35, 83)

        records.append({
            "ssn":                   ssn,
            "customer_id":           f"CUST-{random.randint(1000,9999)}",
            "report_date":           date.today().isoformat(),
            "bureau_name":           random.choice(BUREAUS),
            "credit_score":          score,
            "credit_score_band":     band(score, SCORE_BANDS),
            "credit_score_change":   random.randint(-50, 50),
            "score_model":           "FICO 8",
            "total_debt":            total_debt,
            "total_credit_limit":    credit_lim,
            "credit_utilization_pct":round((total_debt/credit_lim)*100, 2) if credit_lim>0 else 0,
            "revolving_debt":        round(total_debt * random.uniform(0.1, 0.4), 2),
            "installment_debt":      round(total_debt * random.uniform(0.1, 0.3), 2),
            "mortgage_debt":         round(total_debt * random.uniform(0.0, 0.5), 2),
            "student_loan_debt":     round(total_debt * random.uniform(0.0, 0.2), 2),
            "auto_loan_debt":        round(total_debt * random.uniform(0.0, 0.2), 2),
            "stated_income":         round(random.uniform(15000, 350000), 2),
            "verified_income":       round(random.uniform(15000, 350000), 2),
            "dti_ratio":             dti,
            "dti_category":          band(int(dti), DTI_CATS),
            "loan_risk_score":       risk_score,
            "risk_category":         band(risk_score, RISK_CATS),
            "bankruptcy_flag":       random.random() < 0.02,
            "bankruptcy_date":       random_date(2010, 2022).isoformat() if random.random()<0.02 else None,
            "foreclosure_flag":      random.random() < 0.01,
            "collections_count":     random.randint(0, 5),
            "delinquency_count":     random.randint(0, 10),
            "late_payments_30d":     random.randint(0, 5),
            "late_payments_60d":     random.randint(0, 3),
            "late_payments_90d":     random.randint(0, 2),
            "open_accounts":         random.randint(1, 20),
            "closed_accounts":       random.randint(0, 15),
            "total_accounts":        random.randint(1, 35),
            "oldest_account_years":  random.randint(1, 30),
            "newest_account_months": random.randint(1, 24),
            "hard_inquiries_6m":     random.randint(0, 6),
            "hard_inquiries_12m":    random.randint(0, 10),
            "record_created_at":     datetime.utcnow().isoformat(),
            "batch_date":            date.today().isoformat(),
            "source_file":           f"credit_bureau_{date.today().isoformat()}.json",
        })
    return records

def upload_to_gcs(data, folder, filename):
    bucket = client.bucket(BUCKET_NAME)
    blob   = bucket.blob(f"{folder}/{filename}")
    blob.upload_from_string(
        json.dumps(data, default=str),
        content_type="application/json"
    )
    print(f"Uploaded {len(data)} records → gs://{BUCKET_NAME}/{folder}/{filename}")

if __name__ == "__main__":
    today    = date.today().isoformat()
    ts       = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    print("=" * 60)
    print("  Ambati Bank — Batch File Generator")
    print(f"  Bucket: {BUCKET_NAME}")
    print("=" * 60)

    print("\nGenerating customers batch (500 records)...")
    customers = generate_customers(500)
    upload_to_gcs(customers, "customers", f"customers_{ts}.json")

    print("\nGenerating credit bureau batch (500 records)...")
    bureau = generate_credit_bureau(500)
    upload_to_gcs(bureau, "credit_bureau", f"credit_bureau_{ts}.json")

    print("\nBatch generation complete!")
    print(f"  gs://{BUCKET_NAME}/customers/customers_{ts}.json")
    print(f"  gs://{BUCKET_NAME}/credit_bureau/credit_bureau_{ts}.json")
