# ADR-001: Medallion Architecture for Steam Analytics

## Status
Accepted

## Context
We need to design a data architecture that enables:

1. Ingesting raw data from Steam APIs (Store, Reviews, Player Stats)
2. Transforming and cleaning data incrementally
3. Building historical records using SCD Type 2 (Steam only exposes current snapshots)
4. Serving aggregated data for analytics and dashboards
5. Maintaining data lineage and traceability

### Constraints
- Microsoft Fabric as the platform (project requirement)
- Steam APIs have rate limits (~200 requests/5min)
- No public historical data for Steam prices/popularity exists
- Limited budget (use capabilities included in Fabric)

## Decision
We will implement **Medallion Architecture** (Bronze → Silver → Gold) on Delta Lake in Microsoft Fabric Lakehouse.

### Bronze Layer
- **Purpose**: Landing zone for raw data (Historical Archive)
- **Format**: Delta Lake (Table)
- **Schema Strategy**:
    - `raw_content` (String/JSON): Stores the full API response payload.
    - `ingestion_metadata` (Struct): Timestamp, Source, BatchID.
    - Prevents ingestion failures due to upstream schema drift.
- **Partitioning**: By `ingestion_date`.

### Silver Layer
- **Purpose**: Clean, typed, historical data
- **Format**: Delta Lake with SCD Type 2
- **Operations**: MERGE to detect changes
- **Validation**: Data contracts with Pydantic + Great Expectations

### Gold Layer
- **Purpose**: Business metrics ready for consumption
- **Format**: Delta Lake optimized (Z-ORDER)
- **Consumers**: Power BI, analysis notebooks

## Consequences

### Positive
- **Reprocessability**: Immutable Bronze allows re-running transformations
- **Auditability**: SCD2 in Silver maintains complete change history
- **Separation of concerns**: Each layer has clear responsibility
- **Evolution**: We can add new sources without affecting downstream layers
- **Debugging**: Easy to identify where data failed

### Negative
- **Storage**: 3 copies of data increases costs (mitigated: Fabric includes storage)
- **Latency**: Data passes through 3 layers before being available
- **Complexity**: More code than a simple 1-layer pipeline

### Risks
- **Schema evolution**: Changes in Steam API require careful handling
- **Mitigation**: Schema versioning, strict contracts at ingestion

## Alternatives Considered

### 1. Direct ELT to a single table
- Rejected: No history, no reprocessability, difficult debugging

### 2. Lambda Architecture (batch + streaming)
- Rejected: Overengineering for our use case (daily ingestion)

### 3. Data Vault 2.0
- Rejected: Excessive complexity for project size

## References
- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Microsoft Fabric Lakehouse](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-overview)
- [Delta Lake Documentation](https://docs.delta.io/)

## Record
- **Date**: 2025-12-15
- **Author**: Eduardo Jafet Cendon Aguilar
