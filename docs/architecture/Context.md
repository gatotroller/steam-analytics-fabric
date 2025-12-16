# Context

## System Context

## Actors

| Actor | Type | Description |
|-------|------|-------------|
| Data Analyst | Person | Consumes dashboards and reports for gaming market analysis |
| Steam APIs | External System | Official Valve APIs that expose Steam platform data |
| Power BI | External System | Visualization tool that consumes the semantic model |

## System: Steam Analytics Platform

**Purpose**: Build a data lakehouse that captures historical Steam data for trend analysis in the video game industry.

**Responsibilities**:
1. Extract data from Steam APIs while respecting rate limits
2. Store raw data immutably (Bronze)
3. Transform and apply SCD Type 2 for historical tracking (Silver)
4. Aggregate business metrics (Gold)
5. Expose semantic model for consumption

**Technology**: Microsoft Fabric (Lakehouse, Spark, Data Pipelines)
