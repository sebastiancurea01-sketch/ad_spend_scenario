# E-commerce Growth Sustainability Analysis
> **Can 174% YoY revenue growth survive 98% customer churn?**  
> I found this signal analysing 3 years of transactional data in Excel + Power BI — then built a production-grade pipeline to quantify and prove it.

---

## dbt DAG
> Full lineage from raw sources to mart models — every dependency is tested and documented.

![dbt DAG](docs/images/dbt_dag.png)
*(screenshot: paste your dbt Cloud lineage graph here)*

---

## The Analysis

**174% YoY growth — but 98% of customers never returned**  
A Maven Analytics e-commerce company grew revenue 174% YoY over 3 years 
across 1.7M+ transactions. Exploratory analysis in Excel + Power BI revealed 
that 98% of customers never came back for a second purchase.

**The problem: growth built entirely on paid acquisition**  
Every dollar of revenue was coming from new customers — with no organic base 
to sustain it. The source data had no marketing spend, making acquisition 
efficiency impossible to evaluate.

**The answer: a synthetic ad spend model to quantify the risk**  
I designed a synthetic ad spend table based on real Google/Bing benchmarks, 
integrated it into the pipeline, and built `scenario__growth_sustainability` 
to quantify exactly when the model breaks.

> *At $29 CAC and $62 LTV, each customer generates only $33 net value 
> after acquisition cost — with 98% churn, there is no second purchase 
> to improve that ratio.*

**Actionable insights:**
- 🎯 Launch retention programme targeting first 30 days post-purchase
- 🎯 Set minimum ROAS threshold of 4x before scaling any channel
- 🎯 Shift 20% of budget to brand campaigns to build organic equity

**Targets to reach sustainability:** Churn <60% · ROAS >4x · LTV/CAC >3x

---

## Architecture

```
                    ┌─────────────────────────────┐
                    │   Source Data (Maven Analytics)│
                    │   Orders · Sessions · Products │
                    └────────────┬────────────────────┘
                                 │ COPY INTO
                    ┌────────────▼────────────────────┐
                    │   Databricks Unity Catalog       │
                    │   Raw Delta Tables               │
                    │   (Future: Azure Data Lake)      │
                    └────────────┬────────────────────┘
                                 │
                    ┌────────────▼────────────────────┐
                    │   dbt Cloud                      │
                    │   Staging → Intermediate → Mart  │
                    └────────────┬────────────────────┘
                                 │
                    ┌────────────▼────────────────────┐
                    │   Power BI Dashboard             │
                    │   7 KPIs · Sustainability Model  │
                    └─────────────────────────────────┘
```

**Future state:** Raw data will be ingested from Azure Data Lake Storage Gen2 
into Databricks, replacing the current COPY INTO from local CSV. This aligns 
the architecture with a standard Azure + Databricks enterprise stack.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Databricks, COPY INTO, Unity Catalog |
| Transformation | dbt Cloud (Databricks adapter) |
| Orchestration | Databricks Workflows |
| BI | Power BI |
| CI/CD | GitHub Actions |
| Data Quality | dbt tests, dbt_expectations |
| Future Ingestion | Azure Data Lake Storage Gen2 |

---

## Dataset
**Source:** Maven Analytics E-commerce Dataset  
**Volume:** 1.7M+ rows — orders, sessions, order items, products, refunds  
**Period:** March 2012 – March 2015  
**Note:** Marketing spend data absent from source. A synthetic `ad_spend` table 
was designed based on real Google/Bing search benchmarks and integrated as a 
first-class source. See [ADR-002](docs/decisions/ADR-002.md).

---

## Models

| Model | Layer | Description |
|---|---|---|
| `stg_orders` | Staging | Cleaned orders, explicit type casts |
| `stg_website_sessions` | Staging | Sessions with UTM attribution |
| `stg_ad_spend` | Staging | Synthetic paid search spend by channel |
| `int_sessions_joined_orders` | Intermediate | Sessions enriched with order data, `is_new_customer` flag |
| `int_daily_performance_summarized` | Intermediate | Daily aggregation by channel — revenue, sessions, conversions |
| `fct_monthly_metrics` | Mart | All 7 KPIs at monthly grain — ROAS, CAC, LTV, CR, YoY, churn, brand % |
| `scenario__growth_sustainability` | Mart | LTV/CAC ratio, months to payback, breakeven ROAS |

---

## Key KPIs (full dataset 2012–2015)

| KPI | Value | Benchmark | Status |
|---|---|---|---|
| YoY Revenue Growth | 174% | >20% | ✅ Strong |
| Churn Rate | 98% | <40% | 🔴 Critical |
| ROAS | ~2x | >4x | 🔴 Underperforming |
| CAC | $29 | — | — |
| LTV | $62 | — | — |
| Net value per customer after CAC | $33 | >3x CAC | 🔴 Unsustainable |

---

## Data Quality
- Generic tests: `not_null`, `unique`, `relationships` on all staging models
- Singular tests:
  - Revenue is never negative
  - New customers never exceed total conversions
  - CAC exceeding LTV triggers a **warning** — this is the finding, not a bug
  - ROAS is always positive when spend exists
  - Ad spend only contains known channels

---

## Key Engineering Decisions

| ADR | Decision |
|---|---|
| [ADR-001](docs/decisions/ADR-001.md) | COPY INTO over Auto Loader — static batch dataset |
| [ADR-002](docs/decisions/ADR-002.md) | Synthetic ad_spend table — no marketing data in source |
| [ADR-003](docs/decisions/ADR-003.md) | Incremental models on fact tables |
| [ADR-004](docs/decisions/ADR-004.md) | Databricks + dbt Cloud as core stack |

---

## Setup

```bash
# 1. Clone the repo
git clone https://github.com/yourusername/your-repo-name.git

# 2. Install dbt dependencies
dbt deps

# 3. Configure profiles.yml with your Databricks credentials

# 4. Run the pipeline
dbt build
```

---

## CI/CD
Every pull request triggers:
1. `sqlfluff lint` — SQL style enforcement
2. `dbt compile` — syntax validation
3. `dbt build --select state:modified+` — tests on changed models only

On merge to `main`: full `dbt build` runs automatically.

---

## Orchestration
Databricks Workflow DAG with on-failure email alert:
```
[01_ingestion] → [02_dbt_build] → [03_quality_checks]
```
