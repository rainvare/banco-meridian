"""
etl_pipeline.py
Banco Meridian — Data Integration Pipeline
Extract → Clean → Resolve Identities → Normalize Currencies → Detect Fraud → Load

Design decisions are logged explicitly so they can be audited.
"""

import csv
import json
import sqlite3
import os
import re
from datetime import datetime, date
from collections import defaultdict

BASE    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW     = os.path.join(BASE, "raw_data")
WH_DIR  = os.path.join(BASE, "warehouse")
DB_PATH = os.path.join(WH_DIR, "banco_meridian.db")
LOG     = []  # audit log of data quality decisions

def log(msg, level="INFO"):
    entry = f"[{datetime.now().strftime('%H:%M:%S')}] [{level}] {msg}"
    print(entry)
    LOG.append(entry)

def read_csv(fname):
    with open(os.path.join(RAW, fname), newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def read_json(fname):
    with open(os.path.join(RAW, fname), encoding="utf-8") as f:
        return json.load(f)

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

DATE_FORMATS = [
    "%Y-%m-%d", "%d/%m/%Y", "%m-%d-%y", "%m/%d/%Y",
    "%Y%m%d", "%d-%m-%Y", "%Y-%m-%dT%H:%M:%S",
]

def parse_date(s):
    """Try multiple date formats. Return ISO date string or None."""
    if not s or str(s).strip() in ("", "INVALID_DATE", "None"):
        return None
    s = str(s).strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    LOG.append(f"[WARN] Could not parse date: '{s}'")
    return None

def parse_amount(s):
    """Strip currency symbols, DR/CR notation. Return float or None."""
    if not s or str(s).strip() == "":
        return None
    s = str(s).strip()
    negative = False
    if s.startswith("-"):
        negative = True
        s = s[1:]
    if "DR" in s:
        negative = True
        s = s.replace("DR","").strip()
    s = s.replace("CR","").strip()
    # Remove currency prefixes like "ARS 1,234.56" or "$1234"
    s = re.sub(r'^[A-Z]{3}\s*', '', s)
    s = s.replace("$","").replace(",","").strip()
    try:
        val = float(s)
        return -val if negative else val
    except ValueError:
        return None

COUNTRY_MAP = {
    "AR":"AR","ARGENTINA":"AR","ARG":"AR",
    "UY":"UY","URUGUAY":"UY","URY":"UY",
    "US":"US","USA":"US","UNITED STATES":"US","ESTADOS UNIDOS":"US",
}

def normalize_country(s):
    if not s:
        return None
    return COUNTRY_MAP.get(str(s).strip().upper())

SEGMENT_MAP = {
    "RETAIL":"retail","PERSONAL":"retail","personal":"retail",
    "PREMIUM":"premium","HIGH VALUE":"premium","high value":"premium",
    "CORPORATE":"corporate","CORP":"corporate","corp":"corporate",
    "SME":"sme","BUSINESS":"sme","business":"sme",
}

def normalize_segment(s):
    if not s:
        return "unknown"
    return SEGMENT_MAP.get(str(s).strip(), str(s).strip().lower())

LOAN_STATUS_MAP = {
    "active":"active","activo":"active","ACTIVE":"active",
    "paid_off":"paid_off","paidoff":"paid_off",
    "defaulted":"defaulted","defualted":"defaulted",
    "restructured":"restructured","written_off":"written_off",
}

def normalize_loan_status(s):
    return LOAN_STATUS_MAP.get(str(s).strip(), str(s).strip().lower())

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1 — FX RATES
# Resolve two providers: use Provider_A as primary, fill gaps with Provider_B
# ═══════════════════════════════════════════════════════════════════════════════

def build_fx_lookup():
    log("FX — Loading exchange rates from two providers")
    rows = read_csv("fx_rates.csv")

    by_date_provider = defaultdict(dict)
    for r in rows:
        by_date_provider[r["date"]][r["provider"]] = {
            "ARS_USD": float(r["ARS_USD"]),
            "UYU_USD": float(r["UYU_USD"]),
        }

    fx = {}
    gaps_filled = 0
    for d, providers in sorted(by_date_provider.items()):
        if "Provider_A" in providers:
            fx[d] = providers["Provider_A"]
        elif "Provider_B" in providers:
            fx[d] = providers["Provider_B"]
            gaps_filled += 1

    log(f"FX — {len(fx)} dates loaded. {gaps_filled} gaps filled with Provider_B")

    # Forward-fill any remaining gaps
    from datetime import timedelta as _td
    all_dates = sorted(fx.keys())
    last    = fx[all_dates[0]]
    filled  = 0
    complete = {}
    current = date.fromisoformat(all_dates[0])
    end     = date.fromisoformat(all_dates[-1])
    while current <= end:
        ds = current.isoformat()
        if ds in fx:
            last = fx[ds]
        else:
            filled += 1
        complete[ds] = last
        current = current + _td(days=1)

    log(f"FX — {filled} additional dates forward-filled")
    return complete

def to_usd(amount, currency, date_str, fx):
    if not amount or not date_str:
        return None
    if currency == "USD":
        return round(amount, 2)
    rate_date = str(date_str)[:10]
    rates = fx.get(rate_date)
    if not rates:
        all_dates = sorted(fx.keys())
        if not all_dates:
            return None
        if rate_date < all_dates[0]:
            rate_date = all_dates[0]
        elif rate_date > all_dates[-1]:
            rate_date = all_dates[-1]
        rates = fx.get(rate_date)
    if not rates:
        return None
    if currency == "ARS":
        return round(amount / rates["ARS_USD"], 2)
    if currency == "UYU":
        return round(amount / rates["UYU_USD"], 2)
    return None

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2 — CUSTOMERS
# Deduplicate, normalize country/segment, build master customer table
# ═══════════════════════════════════════════════════════════════════════════════

def clean_customers():
    log("CUSTOMERS — Cleaning core banking customer records")
    raw = read_csv("core_banking_customers.csv")

    seen_ids   = {}
    duplicates = 0
    cleaned    = []

    for r in raw:
        cid = r["CUST_ID"].strip()
        if cid in seen_ids:
            duplicates += 1
            continue
        seen_ids[cid] = True

        country = normalize_country(r["PAIS"])
        if not country:
            log(f"CUSTOMERS — Unknown country '{r['PAIS']}' for {cid}, skipping", "WARN")
            continue

        cleaned.append({
            "customer_id":  cid,
            "first_name":   r["NOMBRE"].strip().title(),
            "last_name":    r["APELLIDO"].strip().title(),
            "dob":          parse_date(r["FECHA_NAC"]),
            "country":      country,
            "city":         r["CIUDAD"].strip().title(),
            "segment":      normalize_segment(r["SEGMENTO"]),
            "registered_at":parse_date(str(r["FECHA_ALTA"])),
            "status":       r["ESTADO"].strip().upper(),
        })

    log(f"CUSTOMERS — {len(cleaned)} clean records. {duplicates} duplicates removed.")
    return cleaned

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3 — CRM IDENTITY RESOLUTION
# CRM uses numeric IDs; link back to core banking via core_ref field
# ═══════════════════════════════════════════════════════════════════════════════

def clean_crm(master_ids):
    log("CRM — Resolving identities against core banking")
    raw = read_csv("crm_clients.csv")

    resolved   = 0
    unresolved = 0
    cleaned    = []

    for r in raw:
        core_ref = r["core_ref"].strip()
        crm_id   = r["id"].strip()

        # Try to resolve: use core_ref if present and valid
        if core_ref and core_ref in master_ids:
            resolved_id = core_ref
            resolved += 1
        else:
            # Try reconstructing: CRM id is numeric part of CXXXXX
            candidate = f"C{int(crm_id):05d}" if crm_id.isdigit() else None
            if candidate and candidate in master_ids:
                resolved_id = candidate
                resolved += 1
                log(f"CRM — Resolved {crm_id} → {resolved_id} via ID reconstruction")
            else:
                resolved_id = None
                unresolved += 1

        nps = r["nps_score"]
        cleaned.append({
            "crm_id":         crm_id,
            "customer_id":    resolved_id,
            "email":          r["email"].strip() or None,
            "phone":          r["phone"].strip() or None,
            "segment_crm":    normalize_segment(r["segment"]),
            "assigned_exec":  r["assigned_exec"].strip() or None,
            "last_contact":   parse_date(str(r["last_contact"])),
            "nps_score":      int(nps) if str(nps).strip().isdigit() else None,
            "churn_risk":     r["churn_risk"].strip().lower(),
            "products_count": int(r["products_count"]) if r["products_count"] else None,
        })

    log(f"CRM — {resolved} resolved, {unresolved} unresolved (orphan CRM records)")
    return cleaned

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4 — ACCOUNTS
# ═══════════════════════════════════════════════════════════════════════════════

def clean_accounts(fx):
    log("ACCOUNTS — Cleaning account records")
    raw = read_csv("core_banking_accounts.csv")
    cleaned = []
    for r in raw:
        balance = parse_amount(r["SALDO"])
        opened  = parse_date(str(r["APERTURA"]))
        balance_usd = to_usd(balance, r["MONEDA"], opened, fx) if balance else None

        cleaned.append({
            "account_id":   r["ACC_NUM"].strip(),
            "customer_id":  r["CUST_ID"].strip(),
            "product":      r["PRODUCTO"].strip().lower(),
            "currency":     r["MONEDA"].strip(),
            "balance":      balance,
            "balance_usd":  balance_usd,
            "opened_at":    opened,
            "status":       r["ESTADO_CTA"].strip().upper(),
        })

    log(f"ACCOUNTS — {len(cleaned)} records cleaned")
    return cleaned

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 5 — TRANSACTIONS + FRAUD DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def clean_transactions(fx, account_map):
    log("TRANSACTIONS — Cleaning and running fraud detection rules")
    raw = read_csv("transactions.csv")

    cleaned        = []
    fraud_detected = 0
    parse_errors   = 0

    # Build per-account transaction history for velocity check
    acc_txn_times  = defaultdict(list)

    for r in raw:
        amount = parse_amount(r["AMOUNT"])
        if amount is None:
            parse_errors += 1
            continue

        ts_raw = r["TIMESTAMP"].strip()
        # Try datetime parse
        ts = None
        for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"]:
            try:
                ts = datetime.strptime(ts_raw, fmt)
                break
            except:
                continue
        if not ts:
            parse_errors += 1
            continue

        acc_id   = r["ACC_NUM"].strip()
        currency = account_map.get(acc_id, {}).get("currency", r["CURRENCY"].strip())
        amount_usd = to_usd(amount, currency, ts.date().isoformat(), fx)

        acc_txn_times[acc_id].append(ts)

        cleaned.append({
            "txn_id":       r["TXN_ID"].strip(),
            "account_id":   acc_id,
            "ts":           ts,
            "date":         ts.date().isoformat(),
            "hour":         ts.hour,
            "txn_type":     r["TYPE"].strip().lower(),
            "amount":       abs(amount),
            "amount_usd":   abs(amount_usd) if amount_usd else None,
            "currency":     currency,
            "channel":      r["CHANNEL"].strip().lower(),
            "status":       r["STATUS"].strip().lower(),
            "description":  r["DESCRIPTION"].strip(),
            "fraud_flag":   0,
            "fraud_reason": "",
        })

    log(f"TRANSACTIONS — {parse_errors} records skipped due to parse errors")

    # ── Fraud Detection Rules ──────────────────────────────────────────────────
    # Rule 1: Already flagged by source system
    for t in cleaned:
        if t["status"] == "flagged" or "[SUSPICIOUS]" in t["description"]:
            t["fraud_flag"] = 1
            t["fraud_reason"] = "flagged_by_source"

    # Rule 2: Unusual hour (2–4 AM) + large amount
    for t in cleaned:
        if t["hour"] in [2, 3, 4] and t["amount_usd"] and t["amount_usd"] > 5000:
            t["fraud_flag"] = 1
            t["fraud_reason"] = (t["fraud_reason"] + "|unusual_hour_large_amount").strip("|")

    # Rule 3: Round large amounts (multiples of 5000)
    for t in cleaned:
        if t["amount_usd"] and t["amount_usd"] >= 5000 and t["amount_usd"] % 5000 == 0:
            t["fraud_flag"] = 1
            t["fraud_reason"] = (t["fraud_reason"] + "|round_large_amount").strip("|")

    # Rule 4: Velocity — more than 5 transactions in 10 minutes
    for acc_id, times in acc_txn_times.items():
        times_sorted = sorted(times)
        for i in range(len(times_sorted)):
            window = [t for t in times_sorted[i:]
                      if (t - times_sorted[i]).total_seconds() <= 600]
            if len(window) >= 5:
                window_ids = set()
                for t in cleaned:
                    if t["account_id"] == acc_id and t["ts"] in window:
                        t["fraud_flag"] = 1
                        t["fraud_reason"] = (t["fraud_reason"] + "|velocity").strip("|")

    fraud_detected = sum(1 for t in cleaned if t["fraud_flag"])
    log(f"TRANSACTIONS — {len(cleaned)} clean. {fraud_detected} fraud flags raised.")
    return cleaned

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6 — LOANS
# ═══════════════════════════════════════════════════════════════════════════════

def clean_loans(fx):
    log("LOANS — Cleaning loan records")
    raw = read_csv("loan_system.csv")
    cleaned = []
    status_corrections = 0

    for r in raw:
        orig_status = r["status"].strip()
        clean_status = normalize_loan_status(orig_status)
        if clean_status != orig_status.lower():
            status_corrections += 1

        principal = parse_amount(r["principal"])
        start     = parse_date(str(r["start_date"]))
        end       = parse_date(str(r["end_date"]))
        principal_usd = to_usd(principal, r["currency"].strip(), start, fx) if principal else None

        cleaned.append({
            "loan_id":        r["loan_id"].strip(),
            "customer_id":    r["customer_ref"].strip(),
            "country":        r["country"].strip(),
            "currency":       r["currency"].strip(),
            "principal":      principal,
            "principal_usd":  principal_usd,
            "interest_rate":  float(r["interest_rate"]) if r["interest_rate"] else None,
            "term_months":    int(r["term_months"]) if r["term_months"] else None,
            "start_date":     start,
            "end_date":       end,
            "status":         clean_status,
            "days_overdue":   int(r["days_overdue"]) if r["days_overdue"] else 0,
            "collateral":     r["collateral"].strip().lower(),
        })

    log(f"LOANS — {len(cleaned)} records. {status_corrections} status values corrected.")
    return cleaned

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 7 — CALL CENTER
# ═══════════════════════════════════════════════════════════════════════════════

def clean_tickets():
    log("TICKETS — Cleaning call center logs")
    raw = read_csv("callcenter_logs.csv")
    cleaned = []

    CATEGORY_MAP = {
        "Account Inquiry": "account_inquiry",
        "Loan Question":   "loan_question",
        "Complaint":       "complaint",
        "Fraud Report":    "fraud_report",
        "Card Blocked":    "card_blocked",
        "Transfer Issue":  "transfer_issue",
        "General":         "general",
        "Technical Support":"technical_support",
    }

    for r in raw:
        cat = r["category"].strip()
        cat_clean = CATEGORY_MAP.get(cat, cat.lower().replace(" ","_"))

        # customer_id may be numeric (without C prefix)
        cid = str(r["customer_id"]).strip()
        if cid.isdigit():
            cid = f"C{int(cid):05d}"

        sat = r["satisfaction"].strip()
        cleaned.append({
            "ticket_id":    r["ticket_id"].strip(),
            "customer_id":  cid,
            "created_at":   parse_date(str(r["created_at"])),
            "resolved_at":  parse_date(str(r["resolved_at"])) if r["resolved_at"] else None,
            "category":     cat_clean,
            "channel":      r["channel"].strip().lower(),
            "agent_id":     r["agent_id"].strip(),
            "resolution":   r["resolution"].strip().lower(),
            "satisfaction": int(sat) if sat.isdigit() else None,
            "country":      r["country"].strip().upper(),
        })

    log(f"TICKETS — {len(cleaned)} records cleaned")
    return cleaned

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 8 — APP EVENTS
# ═══════════════════════════════════════════════════════════════════════════════

def clean_app_events():
    log("APP EVENTS — Cleaning mobile event log")
    raw = read_json("app_events.json")
    cleaned  = []
    rejected = 0

    for r in raw:
        cid = r.get("customer_id")
        ts  = r.get("timestamp","")

        if not cid or ts == "INVALID_DATE":
            rejected += 1
            continue

        meta = r.get("metadata", {})
        cleaned.append({
            "event_id":      r["event_id"],
            "customer_id":   str(cid).strip(),
            "ts":            parse_date(ts),
            "event_type":    r.get("event_type","").lower(),
            "device":        r.get("device","").lower(),
            "app_version":   r.get("app_version",""),
            "session_id":    r.get("session_id",""),
            "amount":        meta.get("amount"),
            "duration_sec":  meta.get("duration_sec"),
            "success":       1 if meta.get("success") else 0,
        })

    log(f"APP EVENTS — {len(cleaned)} clean. {rejected} rejected (malformed).")
    return cleaned

# ═══════════════════════════════════════════════════════════════════════════════
# LOAD — Write to SQLite Data Warehouse
# ═══════════════════════════════════════════════════════════════════════════════

def load_warehouse(customers, crm, accounts, transactions, loans, tickets, app_events, fx):
    log("LOAD — Writing to data warehouse")

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.executescript("""
    PRAGMA foreign_keys = ON;

    CREATE TABLE dim_customer (
        customer_id   TEXT PRIMARY KEY,
        first_name    TEXT, last_name TEXT,
        dob           TEXT, country TEXT, city TEXT,
        segment       TEXT, registered_at TEXT, status TEXT,
        -- CRM enrichment
        email TEXT, phone TEXT, nps_score INTEGER,
        churn_risk TEXT, assigned_exec TEXT, last_contact TEXT
    );

    CREATE TABLE dim_account (
        account_id  TEXT PRIMARY KEY,
        customer_id TEXT REFERENCES dim_customer(customer_id),
        product     TEXT, currency TEXT,
        balance     REAL, balance_usd REAL,
        opened_at   TEXT, status TEXT
    );

    CREATE TABLE fact_transactions (
        txn_id       TEXT PRIMARY KEY,
        account_id   TEXT REFERENCES dim_account(account_id),
        customer_id  TEXT REFERENCES dim_customer(customer_id),
        date         TEXT, hour INTEGER,
        txn_type     TEXT, amount REAL, amount_usd REAL,
        currency     TEXT, channel TEXT, status TEXT,
        fraud_flag   INTEGER DEFAULT 0,
        fraud_reason TEXT
    );

    CREATE TABLE fact_loans (
        loan_id       TEXT PRIMARY KEY,
        customer_id   TEXT REFERENCES dim_customer(customer_id),
        country       TEXT, currency TEXT,
        principal     REAL, principal_usd REAL,
        interest_rate REAL, term_months INTEGER,
        start_date    TEXT, end_date TEXT,
        status        TEXT, days_overdue INTEGER, collateral TEXT
    );

    CREATE TABLE fact_support_tickets (
        ticket_id   TEXT PRIMARY KEY,
        customer_id TEXT REFERENCES dim_customer(customer_id),
        created_at  TEXT, resolved_at TEXT,
        category    TEXT, channel TEXT,
        agent_id    TEXT, resolution TEXT,
        satisfaction INTEGER, country TEXT
    );

    CREATE TABLE fact_app_events (
        event_id    TEXT PRIMARY KEY,
        customer_id TEXT REFERENCES dim_customer(customer_id),
        date        TEXT, event_type TEXT, device TEXT,
        app_version TEXT, session_id TEXT,
        amount      REAL, duration_sec INTEGER, success INTEGER
    );

    CREATE TABLE dim_fx_rates (
        date TEXT, currency TEXT, rate_to_usd REAL,
        PRIMARY KEY (date, currency)
    );
    """)

    # Merge CRM into customer records
    crm_by_customer = {r["customer_id"]: r for r in crm if r["customer_id"]}

    cust_rows = []
    for c in customers:
        crm_data = crm_by_customer.get(c["customer_id"], {})
        cust_rows.append((
            c["customer_id"], c["first_name"], c["last_name"],
            c["dob"], c["country"], c["city"],
            c["segment"], c["registered_at"], c["status"],
            crm_data.get("email"), crm_data.get("phone"),
            crm_data.get("nps_score"), crm_data.get("churn_risk"),
            crm_data.get("assigned_exec"), crm_data.get("last_contact"),
        ))
    cur.executemany("INSERT OR REPLACE INTO dim_customer VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", cust_rows)
    log(f"  ✓ dim_customer: {len(cust_rows)} rows")

    acc_rows = [(a["account_id"], a["customer_id"], a["product"], a["currency"],
                 a["balance"], a["balance_usd"], a["opened_at"], a["status"])
                for a in accounts]
    cur.executemany("INSERT OR REPLACE INTO dim_account VALUES (?,?,?,?,?,?,?,?)", acc_rows)
    log(f"  ✓ dim_account: {len(acc_rows)} rows")

    # Build account→customer map for transactions
    acc_cust = {a["account_id"]: a["customer_id"] for a in accounts}

    txn_rows = [(t["txn_id"], t["account_id"], acc_cust.get(t["account_id"]),
                 t["date"], t["hour"], t["txn_type"],
                 t["amount"], t["amount_usd"], t["currency"],
                 t["channel"], t["status"], t["fraud_flag"], t["fraud_reason"])
                for t in transactions]
    cur.executemany("INSERT OR REPLACE INTO fact_transactions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", txn_rows)
    log(f"  ✓ fact_transactions: {len(txn_rows)} rows")

    loan_rows = [(l["loan_id"], l["customer_id"], l["country"], l["currency"],
                  l["principal"], l["principal_usd"], l["interest_rate"],
                  l["term_months"], l["start_date"], l["end_date"],
                  l["status"], l["days_overdue"], l["collateral"])
                 for l in loans]
    cur.executemany("INSERT OR REPLACE INTO fact_loans VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", loan_rows)
    log(f"  ✓ fact_loans: {len(loan_rows)} rows")

    tkt_rows = [(t["ticket_id"], t["customer_id"], t["created_at"], t["resolved_at"],
                 t["category"], t["channel"], t["agent_id"], t["resolution"],
                 t["satisfaction"], t["country"])
                for t in tickets]
    cur.executemany("INSERT OR REPLACE INTO fact_support_tickets VALUES (?,?,?,?,?,?,?,?,?,?)", tkt_rows)
    log(f"  ✓ fact_support_tickets: {len(tkt_rows)} rows")

    evt_rows = [(e["event_id"], e["customer_id"], e["ts"], e["event_type"],
                 e["device"], e["app_version"], e["session_id"],
                 e["amount"], e["duration_sec"], e["success"])
                for e in app_events]
    cur.executemany("INSERT OR REPLACE INTO fact_app_events VALUES (?,?,?,?,?,?,?,?,?,?)", evt_rows)
    log(f"  ✓ fact_app_events: {len(evt_rows)} rows")

    # FX rates flat table
    fx_rows = []
    for d, rates in fx.items():
        fx_rows.append((d, "ARS", rates["ARS_USD"]))
        fx_rows.append((d, "UYU", rates["UYU_USD"]))
    cur.executemany("INSERT OR REPLACE INTO dim_fx_rates VALUES (?,?,?)", fx_rows)
    log(f"  ✓ dim_fx_rates: {len(fx_rows)} rows")

    conn.commit()
    conn.close()
    log(f"  Database → {DB_PATH}")

    # Write audit log
    log_path = os.path.join(WH_DIR, "etl_audit.log")
    with open(log_path, "w") as f:
        f.write("\n".join(LOG))
    print(f"\n  Audit log → {log_path}")

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("Banco Meridian — Data Integration Pipeline")
    print("=" * 60)

    os.makedirs(WH_DIR, exist_ok=True)

    fx           = build_fx_lookup()
    customers    = clean_customers()
    master_ids   = {c["customer_id"] for c in customers}
    crm          = clean_crm(master_ids)
    accounts     = clean_accounts(fx)
    account_map  = {a["account_id"]: {"currency": a["currency"]} for a in accounts}
    transactions = clean_transactions(fx, account_map)
    loans        = clean_loans(fx)
    tickets      = clean_tickets()
    app_events   = clean_app_events()

    load_warehouse(customers, crm, accounts, transactions,
                   loans, tickets, app_events, fx)

    print("=" * 60)
    print("Pipeline complete ✓")
