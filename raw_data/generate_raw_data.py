"""
generate_raw_data.py
Generates synthetic DIRTY data simulating 6 legacy systems of Banco Meridian.
Each source has realistic inconsistencies: format mismatches, duplicate IDs,
missing fields, encoding issues, and date format chaos.
Fraud patterns are seeded into transactions.
"""

import csv
import json
import random
import os
from datetime import date, datetime, timedelta
from copy import deepcopy

random.seed(99)

OUT = os.path.dirname(os.path.abspath(__file__))

# ── Constants ─────────────────────────────────────────────────────────────────
COUNTRIES = {
    "AR": {"currency": "ARS", "city_pool": ["Buenos Aires","Córdoba","Rosario","Mendoza","La Plata"]},
    "UY": {"currency": "UYU", "city_pool": ["Montevideo","Salto","Paysandú","Colonia","Maldonado"]},
    "US": {"currency": "USD", "city_pool": ["Miami","New York","Los Angeles","Houston","Chicago"]},
}

FIRST_NAMES = ["Santiago","Valentina","Mateo","Camila","Nicolás","Lucía","Sebastián",
               "Martina","Tomás","Sofía","Agustín","Isabella","Diego","Emma","Facundo",
               "Carolina","Rodrigo","Florencia","Andres","Renata","James","Emily",
               "Michael","Sarah","Robert","Jennifer","David","Lisa","Daniel","Maria"]
LAST_NAMES  = ["García","Rodríguez","González","Fernández","López","Martínez","Pérez",
               "Sánchez","Romero","Torres","Smith","Johnson","Williams","Brown","Jones",
               "Da Silva","Dos Santos","Oliveira","Souza","Costa"]

PRODUCTS    = ["checking_account","savings_account","personal_loan",
               "mortgage","credit_card","investment_fund","insurance"]
SEGMENTS    = ["retail","premium","corporate","sme"]
EXEC_NAMES  = ["Ana Flores","Carlos Vega","Pilar Ruiz","Jorge Méndez",
               "Laura Chen","Tom Bradley","María Ibáñez","Pablo Reyes"]

N_CUSTOMERS  = 800
N_ACCOUNTS   = 1200
N_TXN        = 15000
N_LOANS      = 400
N_TICKETS    = 1200
N_APP_EVENTS = 8000

START = date(2021, 1, 1)
END   = date(2024, 12, 31)

def rand_date(s=START, e=END):
    return s + timedelta(days=random.randint(0, (e-s).days))

def rand_dt(s=START, e=END):
    d = rand_date(s, e)
    return datetime(d.year, d.month, d.day,
                    random.randint(0,23), random.randint(0,59), random.randint(0,59))

def write_csv(fname, fieldnames, rows):
    p = os.path.join(OUT, fname)
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  ✓ {fname}: {len(rows):,} rows")

def write_json(fname, data):
    p = os.path.join(OUT, fname)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, default=str, indent=2)
    print(f"  ✓ {fname}: {len(data):,} records")

# ── 1. CORE BANKING SYSTEM ─────────────────────────────────────────────────────
# Dirty patterns: date as DD/MM/YYYY or MM-DD-YY, amount with $ sign,
# country codes inconsistent (AR / Argentina / arg), duplicate rows ~2%
print("\n[1/6] Core Banking System")

customers_core = []
for i in range(1, N_CUSTOMERS + 1):
    country = random.choice(list(COUNTRIES.keys()))
    city    = random.choice(COUNTRIES[country]["city_pool"])
    dob     = rand_date(date(1950,1,1), date(2000,1,1))

    # Dirty: date format varies
    if random.random() < 0.4:
        dob_str = dob.strftime("%d/%m/%Y")
    elif random.random() < 0.5:
        dob_str = dob.strftime("%m-%d-%y")
    else:
        dob_str = str(dob)

    # Dirty: country name inconsistent
    country_raw = random.choice([
        country,
        {"AR":"Argentina","UY":"Uruguay","US":"United States"}[country],
        {"AR":"arg","UY":"ury","US":"usa"}[country],
    ])

    customers_core.append({
        "CUST_ID":       f"C{i:05d}",
        "NOMBRE":        random.choice(FIRST_NAMES),
        "APELLIDO":      random.choice(LAST_NAMES),
        "FECHA_NAC":     dob_str,
        "PAIS":          country_raw,
        "CIUDAD":        city,
        "SEGMENTO":      random.choice(SEGMENTS).upper(),   # dirty: sometimes lowercase later
        "FECHA_ALTA":    rand_date(date(2015,1,1), START),
        "ESTADO":        random.choice(["A","A","A","I","S"]),  # Active/Inactive/Suspended
    })

# Inject ~2% duplicates
dupes = random.sample(customers_core, k=int(N_CUSTOMERS * 0.02))
for d in dupes:
    row = deepcopy(d)
    row["NOMBRE"] = row["NOMBRE"].lower()   # slight variation
    customers_core.append(row)

accounts = []
for i in range(1, N_ACCOUNTS + 1):
    owner = random.choice(customers_core)
    country = None
    for k,v in {"AR":"Argentina","UY":"Uruguay","US":"United States"}.items():
        if owner["PAIS"] in [k, v, k.lower()]:
            country = k
    if not country:
        country = random.choice(list(COUNTRIES.keys()))
    currency = COUNTRIES[country]["currency"]
    balance  = round(random.uniform(-500, 200000), 2)

    # Dirty: balance sometimes has currency symbol
    bal_str = f"${balance}" if random.random() < 0.3 else str(balance)

    accounts.append({
        "ACC_NUM":    f"ACC{i:06d}",
        "CUST_ID":    owner["CUST_ID"],
        "PRODUCTO":   random.choice(PRODUCTS[:2]),
        "MONEDA":     currency,
        "SALDO":      bal_str,
        "APERTURA":   rand_date(date(2015,1,1), END),
        "ESTADO_CTA": random.choice(["ACTIVA","ACTIVA","ACTIVA","CERRADA","BLOQUEADA"]),
    })

write_csv("core_banking_customers.csv",
          ["CUST_ID","NOMBRE","APELLIDO","FECHA_NAC","PAIS","CIUDAD","SEGMENTO","FECHA_ALTA","ESTADO"],
          customers_core)
write_csv("core_banking_accounts.csv",
          ["ACC_NUM","CUST_ID","PRODUCTO","MONEDA","SALDO","APERTURA","ESTADO_CTA"],
          accounts)

# ── 2. TRANSACTIONS ────────────────────────────────────────────────────────────
# Dirty: timestamps in different formats, some missing, amount sign inconsistent
# Fraud seeds: velocity (many txn in short window), cross-border anomalies, round amounts
print("\n[2/6] Transaction Ledger")

txn_types  = ["transfer","payment","withdrawal","deposit","purchase","fee"]
channels   = ["branch","atm","online","mobile","pos"]

transactions = []
fraud_ids    = set()

# Select ~1.5% accounts as fraud accounts
fraud_accounts = set(random.sample([a["ACC_NUM"] for a in accounts],
                                    k=max(1, int(N_ACCOUNTS * 0.015))))

for i in range(1, N_TXN + 1):
    acc    = random.choice(accounts)
    is_fraud = acc["ACC_NUM"] in fraud_accounts and random.random() < 0.4
    dt     = rand_dt()
    amount = round(random.uniform(1, 50000), 2)
    txn_type = random.choice(txn_types)

    if is_fraud:
        # Fraud pattern 1: round large amounts
        if random.random() < 0.5:
            amount = random.choice([5000, 10000, 15000, 20000, 50000])
        # Fraud pattern 2: unusual hour (2-4 AM)
        dt = dt.replace(hour=random.randint(2, 4))
        fraud_ids.add(f"T{i:07d}")

    # Dirty: timestamp format varies
    if random.random() < 0.35:
        ts = dt.strftime("%d/%m/%Y %H:%M")
    elif random.random() < 0.5:
        ts = dt.isoformat()
    else:
        ts = dt.strftime("%Y%m%d%H%M%S")

    # Dirty: some amounts negative for debits, some use DR/CR notation
    if txn_type in ["withdrawal","payment","fee"] and random.random() < 0.5:
        amount_str = f"-{amount}"
    elif random.random() < 0.2:
        amount_str = f"{amount} {'DR' if txn_type in ['withdrawal','payment'] else 'CR'}"
    else:
        amount_str = str(amount)

    transactions.append({
        "TXN_ID":      f"T{i:07d}",
        "ACC_NUM":     acc["ACC_NUM"],
        "TIMESTAMP":   ts,
        "TYPE":        txn_type,
        "AMOUNT":      amount_str,
        "CURRENCY":    acc["MONEDA"],
        "CHANNEL":     random.choice(channels),
        "DESCRIPTION": f"{'[SUSPICIOUS] ' if is_fraud and random.random()<0.1 else ''}{txn_type.upper()} {random.randint(100,999)}",
        "STATUS":      "completed" if not is_fraud or random.random() > 0.1 else "flagged",
    })

write_csv("transactions.csv",
          ["TXN_ID","ACC_NUM","TIMESTAMP","TYPE","AMOUNT","CURRENCY","CHANNEL","DESCRIPTION","STATUS"],
          transactions)
print(f"    → {len(fraud_ids)} fraud transactions seeded")

# ── 3. CRM SYSTEM ─────────────────────────────────────────────────────────────
# Dirty: uses different customer ID format (numeric only), missing exec assignments,
# segment names differ from core banking
print("\n[3/6] CRM System")

crm_records = []
core_ids = [c["CUST_ID"] for c in customers_core]

for i, cid in enumerate(core_ids[:N_CUSTOMERS]):  # skip dupes
    # Dirty: CRM uses numeric ID without prefix
    crm_id = str(int(cid[1:]))  # "C00042" → "42"

    # Dirty: segment naming different from core
    seg_map = {"RETAIL":"Personal","PREMIUM":"High Value","CORPORATE":"Corp","SME":"Business"}
    seg_core = customers_core[i]["SEGMENTO"]
    crm_seg  = seg_map.get(seg_core, seg_core)
    if random.random() < 0.15:
        crm_seg = crm_seg.lower()  # inconsistent casing

    # ~10% missing exec
    exec_name = random.choice(EXEC_NAMES) if random.random() > 0.1 else ""

    last_contact = rand_date(date(2022,1,1), END)

    crm_records.append({
        "id":              crm_id,
        "core_ref":        cid if random.random() > 0.05 else "",  # 5% missing link
        "full_name":       f"{customers_core[i]['NOMBRE']} {customers_core[i]['APELLIDO']}",
        "email":           f"client{crm_id}@email.com" if random.random() > 0.08 else "",
        "phone":           f"+{random.randint(1,598)}{random.randint(10000000,99999999)}" if random.random() > 0.12 else "",
        "segment":         crm_seg,
        "assigned_exec":   exec_name,
        "last_contact":    last_contact,
        "nps_score":       random.randint(0,10) if random.random() > 0.2 else "",
        "churn_risk":      random.choice(["low","low","medium","high"]),
        "products_count":  random.randint(1,5),
    })

write_csv("crm_clients.csv",
          ["id","core_ref","full_name","email","phone","segment",
           "assigned_exec","last_contact","nps_score","churn_risk","products_count"],
          crm_records)

# ── 4. LOAN SYSTEM ────────────────────────────────────────────────────────────
# Dirty: loan IDs with different prefix per country, amount in local currency,
# some records have typos in status
print("\n[4/6] Loan System")

loan_statuses = ["active","active","active","paid_off","defaulted","restructured","written_off"]
prefixes = {"AR": "PR", "UY": "CR", "US": "LN"}

loans = []
for i in range(1, N_LOANS + 1):
    cust  = random.choice(customers_core[:N_CUSTOMERS])
    # Determine country from customer
    pais  = cust["PAIS"]
    country = "AR" if pais in ["AR","Argentina","arg"] else \
              "UY" if pais in ["UY","Uruguay","ury"] else "US"
    prefix  = prefixes[country]
    currency = COUNTRIES[country]["currency"]

    principal   = round(random.uniform(1000, 500000), 2)
    rate        = round(random.uniform(0.05, 0.45), 4)
    term_months = random.choice([12, 24, 36, 48, 60, 120, 180, 240])
    start_dt    = rand_date(date(2018,1,1), END)
    end_dt      = start_dt + timedelta(days=term_months*30)
    status      = random.choice(loan_statuses)

    # Dirty: some status values have typos
    if random.random() < 0.05:
        status = random.choice(["Activo","ACTIVE","defualted","paidoff"])

    # Dirty: amount format inconsistent
    if random.random() < 0.25:
        principal_str = f"{currency} {principal:,.2f}"
    else:
        principal_str = str(principal)

    loans.append({
        "loan_id":        f"{prefix}{i:06d}",
        "customer_ref":   cust["CUST_ID"],
        "country":        country,
        "currency":       currency,
        "principal":      principal_str,
        "interest_rate":  rate,
        "term_months":    term_months,
        "start_date":     start_dt,
        "end_date":       end_dt,
        "status":         status,
        "days_overdue":   random.randint(0,365) if status in ["defaulted","defualted"] else 0,
        "collateral":     random.choice(["none","property","vehicle","guarantee","none","none"]),
    })

write_csv("loan_system.csv",
          ["loan_id","customer_ref","country","currency","principal","interest_rate",
           "term_months","start_date","end_date","status","days_overdue","collateral"],
          loans)

# ── 5. CALL CENTER LOGS ────────────────────────────────────────────────────────
# Semi-structured: mixed fields, free text, inconsistent category names
print("\n[5/6] Call Center Logs")

categories = ["account_inquiry","loan_question","complaint","fraud_report",
              "card_blocked","transfer_issue","general","technical_support"]
resolutions = ["resolved","escalated","pending","closed_no_action","callback_scheduled"]

tickets = []
for i in range(1, N_TICKETS + 1):
    cust    = random.choice(customers_core[:N_CUSTOMERS])
    cat     = random.choice(categories)
    created = rand_dt(date(2021,1,1), END)
    minutes = random.randint(2, 180)
    resolved_dt = created + timedelta(minutes=minutes) if random.random() > 0.15 else None

    # Dirty: category sometimes free text variation
    if random.random() < 0.2:
        cat = cat.replace("_"," ").title()

    tickets.append({
        "ticket_id":       f"TKT{i:06d}",
        "customer_id":     cust["CUST_ID"] if random.random() > 0.07 else int(cust["CUST_ID"][1:]),
        "created_at":      created,
        "resolved_at":     resolved_dt if resolved_dt else "",
        "category":        cat,
        "channel":         random.choice(["phone","chat","email","branch"]),
        "agent_id":        f"AGT{random.randint(1,30):03d}",
        "resolution":      random.choice(resolutions),
        "satisfaction":    random.randint(1,5) if random.random() > 0.35 else "",
        "notes":           f"Customer called re: {cat}. " + ("Fraud suspected." if "fraud" in cat else ""),
        "country":         random.choice(list(COUNTRIES.keys())),
    })

write_csv("callcenter_logs.csv",
          ["ticket_id","customer_id","created_at","resolved_at","category",
           "channel","agent_id","resolution","satisfaction","notes","country"],
          tickets)

# ── 6. MOBILE APP EVENTS ──────────────────────────────────────────────────────
# JSON format, nested structure, some malformed records
print("\n[6/6] Mobile App Events")

event_types = ["login","logout","view_balance","transfer","pay_bill",
               "open_account","apply_loan","view_statement","update_profile"]

app_events = []
for i in range(1, N_APP_EVENTS + 1):
    cust   = random.choice(customers_core[:N_CUSTOMERS])
    etype  = random.choice(event_types)
    dt     = rand_dt(date(2022,1,1), END)
    device = random.choice(["android","ios","ios","android","android"])

    event = {
        "event_id":    f"EVT{i:08d}",
        "timestamp":   dt.isoformat(),
        "customer_id": cust["CUST_ID"],
        "event_type":  etype,
        "device":      device,
        "app_version": f"{random.randint(2,5)}.{random.randint(0,9)}.{random.randint(0,9)}",
        "session_id":  f"SES{random.randint(100000,999999)}",
        "metadata": {
            "amount":   round(random.uniform(10,10000),2) if etype in ["transfer","pay_bill"] else None,
            "duration_sec": random.randint(5, 600),
            "success":  random.random() > 0.08,
        }
    }
    # Dirty: ~3% malformed (missing customer_id or corrupted timestamp)
    if random.random() < 0.03:
        if random.random() < 0.5:
            event["customer_id"] = None
        else:
            event["timestamp"] = "INVALID_DATE"

    app_events.append(event)

write_json("app_events.json", app_events)

# ── 7. FX RATES ───────────────────────────────────────────────────────────────
# Two providers with slightly different rates, gaps in dates
print("\n[7] FX Rates (2 providers)")

fx_rows = []
current = date(2021,1,1)
ars_base = 100.0
uyu_base = 43.0

while current <= END:
    ars_base *= random.uniform(0.995, 1.025)  # ARS devaluation trend
    uyu_base *= random.uniform(0.999, 1.008)

    for provider in ["Provider_A", "Provider_B"]:
        # Provider B has ~8% missing dates
        if provider == "Provider_B" and random.random() < 0.08:
            current += timedelta(days=1)
            continue
        # Slight rate difference between providers
        spread = random.uniform(0.98, 1.02)
        fx_rows.append({
            "date":       current,
            "provider":   provider,
            "ARS_USD":    round(ars_base * spread, 4),
            "UYU_USD":    round(uyu_base * spread, 4),
            "source":     provider,
        })
    current += timedelta(days=1)

write_csv("fx_rates.csv",
          ["date","provider","ARS_USD","UYU_USD","source"],
          fx_rows)

print(f"\n{'='*50}")
print("Raw data generation complete.")
print(f"Output directory: {OUT}")
