# Banco Meridian — Multi-Source Banking Data Integration Pipeline

## The Problem

Most banking data projects start with a clean dataset. Real banking infrastructure doesn't work that way.

Banco Meridian is a fictional mid-sized Latin American bank operating across **Argentina, Uruguay, and the United States**. Over 15 years of growth, it accumulated six disconnected systems that were never designed to talk to each other:

- A **core banking system** that stores customer and account data with inconsistent country codes, three different date formats, and ~2% duplicate records from a failed migration
- A **transaction ledger** with 15,000 records spread across three timestamp formats, amounts mixing positive/negative notation with DR/CR flags, and fraud patterns seeded throughout
- A **CRM** that uses a completely different customer ID scheme (numeric) from the core system (`C#####`), with 5% of records missing the linking field entirely
- A **loan origination system** where principals are stored as strings with embedded currency prefixes, and status values have eight variants including typos like `"defualted"` and `"paidoff"`
- A **call center log** with free-text category fields that mean the same thing written six different ways
- A **mobile app event stream** in JSON with nested metadata and ~3% malformed records

On top of this, the bank operates in three currencies (ARS, UYU, USD) and sources its FX rates from two providers — both with gaps and slightly different rates on the same dates.

**The business questions that can't be answered without solving this first:**
- What is the actual loan portfolio value in USD across all three countries?
- Which high-value customers are at risk of churning?
- Where is fraud concentrated — by channel, country, time of day?
- What is the non-performing loan (NPL) rate per country?
- How does digital adoption differ across markets?

None of these are answerable until the data is integrated, cleaned, and normalized.

---

## The Solution

This project builds a full data integration pipeline that takes all six dirty sources and produces a clean, unified, queryable data warehouse — with a complete audit trail of every decision made along the way.

The pipeline solves five distinct problems:

**1. Identity Resolution**
The CRM and core banking systems use different ID formats. The pipeline resolves identities by first matching on the explicit `core_ref` field, then reconstructing the core ID from the numeric CRM ID when the link is missing. Every resolved record is logged.

**2. Format Normalization**
Dates arrive in `YYYY-MM-DD`, `DD/MM/YYYY`, `MM-DD-YY`, and `YYYYMMDDHHMMSS`. Amounts arrive with `$` signs, currency prefixes (`ARS 1,234.56`), `DR`/`CR` notation, and missing values. Country codes arrive as `AR`, `Argentina`, and `arg`. All of it is parsed, standardized, and validated before loading.

**3. Multi-Currency Normalization**
FX rates from two providers are reconciled — Provider A as primary, Provider B as fallback for gaps — and forward-filled for any remaining missing dates. All monetary values (account balances, loan principals, transaction amounts) are converted to USD at the rate corresponding to their transaction date.

**4. Fraud Detection**
Four rule-based patterns are applied across all transactions:
- Transactions already flagged by the source system
- Large amounts (> $5,000 USD) processed between 2–4 AM
- Round amounts divisible by $5,000 (structuring pattern)
- 5+ transactions from the same account within a 10-minute window (velocity)

Each flagged transaction carries a `fraud_reason` field that can carry multiple labels, enabling downstream analysis by rule type.

**5. Audit Logging**
Every data quality decision — duplicates removed, records rejected, statuses corrected, identities reconstructed — is written to `warehouse/etl_audit.log` with timestamp and count. This is the traceability layer that makes the pipeline auditable, not just functional.

---

## Technical Architecture

```
banco-meridian/
├── raw_data/
│   ├── generate_raw_data.py     # Synthetic dirty data generator
│   ├── core_banking_customers.csv
│   ├── core_banking_accounts.csv
│   ├── transactions.csv
│   ├── crm_clients.csv
│   ├── loan_system.csv
│   ├── callcenter_logs.csv
│   ├── app_events.json
│   └── fx_rates.csv
├── etl/
│   └── etl_pipeline.py          # Full ETL: extract → clean → resolve → load
├── warehouse/
│   └── etl_audit.log            # Data quality audit trail (generated)
└── analysis/
    └── queries.sql              # 10 analytical KPI queries
```

### Data Warehouse Schema

```
                    dim_customer
                         │
          ┌──────────────┼──────────────────┐
          │              │                  │
  fact_transactions  fact_loans   fact_support_tickets
          │
  fact_app_events

  dim_account → fact_transactions
  dim_fx_rates (lookup, not joined)
```

| Table | Type | Rows | Description |
|-------|------|------|-------------|
| `dim_customer` | Dimension | 800 | Master customer record, enriched with CRM data |
| `dim_account` | Dimension | 1,200 | Bank accounts with USD-normalized balances |
| `dim_fx_rates` | Dimension | 2,922 | Daily FX rates (ARS/USD, UYU/USD) |
| `fact_transactions` | Fact | 15,000 | All transactions with fraud flags |
| `fact_loans` | Fact | 400 | Loan portfolio with USD principals |
| `fact_support_tickets` | Fact | 1,200 | Call center interactions |
| `fact_app_events` | Fact | 7,744 | Mobile app usage events |

### ETL Pipeline Steps

```
1. FX Resolution        Merge two providers → fill gaps → build date lookup
2. Customer Cleaning    Deduplicate → normalize country/segment
3. CRM Resolution       Match numeric CRM IDs → core C##### format
4. Account Cleaning     Parse dirty balances → convert to USD
5. Transaction ETL      Parse 3 timestamp formats → apply 4 fraud rules
6. Loan Cleaning        Normalize 8 status variants → convert to USD
7. Ticket Cleaning      Standardize free-text categories → fix ID format
8. App Event ETL        Reject malformed records → flatten JSON
9. Load                 Write to SQLite DW → write audit log
```

### Source Data Quality Issues (by design)

| Issue | Source | Volume |
|-------|--------|--------|
| Duplicate customer records | Core banking | ~2% |
| Inconsistent date formats | Core banking, transactions | 3 formats |
| Amounts with currency symbols / DR-CR notation | Accounts, transactions | ~30% |
| Inconsistent country codes | Core banking | 3 variants per country |
| CRM-Core ID mismatch | CRM | 5% missing link |
| Status field typos | Loans | ~2% |
| Malformed JSON records | App events | ~3% |
| FX provider gaps | FX rates | ~8% on Provider B |

---

## Fraud Detection Logic

| Rule | Condition | Pattern |
|------|-----------|---------|
| `flagged_by_source` | status = `flagged` OR description contains `[SUSPICIOUS]` | Source-level flag |
| `unusual_hour_large_amount` | Hour 2–4 AM AND amount_usd > 5,000 | Behavioral anomaly |
| `round_large_amount` | amount_usd ≥ 5,000 AND amount_usd % 5,000 = 0 | Structuring / layering |
| `velocity` | 5+ transactions from same account within 10 minutes | Rapid-fire pattern |

Flags are **additive**: a transaction can carry multiple labels (e.g., `unusual_hour_large_amount|round_large_amount`), enabling compound-rule analysis in SQL.

---

## KPI Queries

The `analysis/queries.sql` file contains 10 production-ready analytical queries:

1. **Executive overview** — portfolio snapshot: customers, deposits, loans, fraud exposure
2. **Portfolio by country** — deposits and loan book broken down by AR / UY / US
3. **Churn risk distribution** — high/medium/low risk customers by country and NPS
4. **Transaction volume by channel and country** — with per-channel fraud rate
5. **Fraud analysis by rule** — exposure in USD per detection pattern
6. **Loan portfolio quality** — status breakdown by country
7. **NPL rate by country** — non-performing loan ratio
8. **Support resolution time** — average resolution minutes and satisfaction by category
9. **Digital adoption** — app usage, device split, and failure rate by country
10. **High-value customers at churn risk** — top customers by deposit value with churn flag

---

## Quick Start

```bash
# 1. Generate dirty source data
python raw_data/generate_raw_data.py

# 2. Run integration pipeline
python etl/etl_pipeline.py

# 3. Review the audit log
cat warehouse/etl_audit.log

# 4. Query the warehouse
python -c "
import sqlite3
conn = sqlite3.connect('warehouse/banco_meridian.db')
cur = conn.cursor()
cur.execute('SELECT country, COUNT(*) FROM dim_customer GROUP BY country')
print(cur.fetchall())
"
```

**Requirements:** Python 3.8+ · No external dependencies

*Swap the SQLite connection string in `etl_pipeline.py` for PostgreSQL, Redshift, or BigQuery with no other changes.*

---

## Sample Output

```
=== Executive Overview ===
active_customers : 480
total_deposits   : $24.4M USD
loan_portfolio   : $11.9M USD
fraud_flagged    : 548 transactions
fraud_exposure   : $15.1M USD

=== NPL Rate by Country ===
AR  →  18.4%
US  →  17.1%
UY  →  15.9%
```

---

*Built by [R. Indira Valentina Réquiz](https://www.linkedin.com/in/indiravalentinarequiz/) · [Portfolio](https://rainvare.github.io/portfolio/)*
