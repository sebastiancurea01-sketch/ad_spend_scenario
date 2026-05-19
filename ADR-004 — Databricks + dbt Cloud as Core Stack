# ADR-004 — Databricks + dbt Cloud as Core Stack
---

## Context
Project requires: scalable compute for large dataset ingestion, a governed 
transformation layer with testing and documentation, and compatibility with 
a standard enterprise cloud stack.

## Decision
Use **Databricks** for ingestion and compute, **dbt Cloud** for transformation.

### Why Databricks over alternatives
| | Databricks | Snowflake | BigQuery |
|---|---|---|---|
| Delta Lake / ACID | ✅ Native | ❌ | ❌ |
| Unity Catalog | ✅ | ❌ | ❌ |
| Azure integration | ✅ Native | Partial | ❌ |
| Spark for large-scale | ✅ | ❌ | ❌ |

Databricks is the standard choice in Azure-centric enterprise environments 
and aligns with the planned Azure Data Lake Gen2 integration.

### Why dbt Cloud over dbt Core
- Managed CI/CD runs without self-hosted infrastructure
- Built-in lineage graph and documentation hosting
- Native Databricks adapter with Unity Catalog support
- Team collaboration features relevant for real-world AE work

## Consequences
- Databricks cost requires cluster management — mitigated with job clusters 
  that auto-terminate
- dbt Cloud adds a subscription cost — free tier sufficient for a portfolio project
- Stack is directly transferable to enterprise Azure + Databricks environments
