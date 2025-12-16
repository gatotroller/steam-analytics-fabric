# ADR-002: SCD Type 2 for Historical Change Tracking

## Status
Accepted

## Context
Steam APIs only expose the **current** state of data:
- Current price of a game
- Current reviews
- Players connected now

There is no public API that exposes historical data. To generate analytical value, we need to build the history ourselves by detecting changes between ingestions.

### Example of the Problem
```
Day 1 - API returns: Cyberpunk 2077, price: $59.99
Day 30 - API returns: Cyberpunk 2077, price: $29.99

Without SCD2: We only know the current price ($29.99)
With SCD2: We know it cost $59.99 from day 1-29, and $29.99 since day 30
```

## Decision
We will implement **SCD Type 2** in the Silver layer for the following entities:

| Entity | Tracked Fields | Justification |
|--------|---------------|---------------|
| `dim_games` | price, discount_pct, description, tags | Pricing strategy analysis |
| `dim_game_reviews` | positive_ratio, total_reviews | Sentiment evolution |

### SCD2 Table Structure
```sql
CREATE TABLE silver.dim_games (
    -- Surrogate key
    game_sk         BIGINT GENERATED ALWAYS AS IDENTITY,

    -- Natural key
    app_id          INT NOT NULL,

    -- Tracked attributes
    name            STRING,
    price_usd       DECIMAL(10,2),
    discount_pct    INT,
    tags            ARRAY<STRING>,

    -- SCD2 metadata
    valid_from      TIMESTAMP NOT NULL,
    valid_to        TIMESTAMP NOT NULL,  -- 9999-12-31 for current
    is_current      BOOLEAN NOT NULL,

    -- Audit fields
    _ingested_at    TIMESTAMP,
    _source_file    STRING,
    _batch_id       STRING
)
USING DELTA
PARTITIONED BY (is_current)
```

### MERGE Logic
```python
MERGE INTO silver.dim_games AS target
USING (staged_changes) AS source
ON target.app_id = source.app_id AND target.is_current = true

-- Close previous record if changes detected
WHEN MATCHED AND (changes detected) THEN
    UPDATE SET
        valid_to = current_timestamp(),
        is_current = false

-- Insert new record
WHEN NOT MATCHED THEN
    INSERT (app_id, ..., valid_from, valid_to, is_current)
    VALUES (source.app_id, ..., current_timestamp(), '9999-12-31', true)
```

## Consequences

### Positive
- **Complete history**: We can answer "what was the price 3 months ago?"
- **Temporal analysis**: Correlate price changes with reviews/sales
- **Unique value**: Data that doesn't exist anywhere else publicly

### Negative
- **Query complexity**: JOINs require filtering by validity date
- **Storage growth**: Each change = new record
- **Expensive MERGE**: Heavier operation than simple INSERT

### Mitigations
- Create `current_*` views that filter only `is_current = true`
- Partition by `is_current` to optimize frequent queries
- Z-ORDER by `app_id` to optimize MERGEs

## Alternatives Considered

### SCD Type 1 (Overwrite)
- Rejected: We lose history, which is the project's main value

### SCD Type 3 (Previous value column)
- Rejected: Only stores 1 previous change, insufficient

### SCD Type 6 (Hybrid 1+2+3)
- Rejected: Unnecessary complexity for our case

## References
- [Kimball SCD Types](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/)
- [Delta Lake MERGE](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge)
