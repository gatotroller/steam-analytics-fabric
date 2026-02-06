# Architecture & Design Decisions

Technical documentation of architectural decisions and design patterns.

---

## Overview

The Steam Analytics Platform follows a **medallion architecture** pattern implemented on Microsoft Fabric, enabling scalable and maintainable data processing.

---

## Design Principles

### 1. Separation of Concerns
```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  EXTRACTION        TRANSFORMATION        PRESENTATION      │
│  (Bronze)          (Silver/Gold)         (Power BI)        │
│                                                             │
│  ┌───────────┐     ┌───────────┐        ┌───────────┐      │
│  │ Extractors│     │ Spark     │        │ Semantic  │      │
│  │ (Python)  │     │ DataFrames│        │ Model     │      │
│  └───────────┘     └───────────┘        └───────────┘      │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 2. Minimal Notebooks, Logic in Python

Notebooks are thin wrappers that:
- Set configuration
- Call Python modules
- Report results

All business logic lives in the `src/steam_analytics/` package.

**Benefits:**
- Testable code
- Version control friendly
- Reusable across notebooks

### 3. Idempotent Operations

All transformations are idempotent:
- Bronze: Append with batch_id deduplication
- Silver: SCD Type 2 with surrogate keys
- Gold: Full refresh (overwrite)

---

## Medallion Architecture

### Bronze Layer (Raw)

**Purpose:** Store raw API responses with minimal transformation.

**Characteristics:**
- JSON data flattened to Delta tables
- Metadata added: `_extracted_at`, `_batch_id`, `_source`
- Partitioned by date for efficient querying
- No data cleaning or validation

**Tables:**
| Table | Source | Partitioning |
|-------|--------|--------------|
| raw_steam_store | Store API | date |
| raw_steam_reviews | Reviews API | date |
| raw_steam_player_stats | Players API | date |
| game_catalog | AppList API | none |

### Silver Layer (Cleaned)

**Purpose:** Clean, validate, and model data with history tracking.

**Characteristics:**
- Data validation and type casting
- SCD Type 2 for dimension tables
- Fact tables with proper timestamps
- Business keys and surrogate keys

**SCD Type 2 Implementation:**
```
┌─────────┬─────────────┬─────────────┬────────────┐
│ game_sk │ valid_from  │ valid_to    │ is_current │
├─────────┼─────────────┼─────────────┼────────────┤
│ 1       │ 2024-01-01  │ 2024-06-15  │ false      │
│ 2       │ 2024-06-15  │ 9999-12-31  │ true       │
└─────────┴─────────────┴─────────────┴────────────┘
```

### Gold Layer (Aggregated)

**Purpose:** Pre-computed aggregations for reporting.

**Characteristics:**
- Denormalized for query performance
- Business metrics calculated
- Ready for Power BI consumption
- Full refresh on each run

---

## Game Catalog System

### Problem

Hardcoding game IDs doesn't scale:
- Steam has 150,000+ games
- New games release daily
- Popularity changes constantly

### Solution

Dynamic catalog with priority-based sync:
```
┌─────────────────────────────────────────────────────────────┐
│                     CATALOG SYSTEM                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  DISCOVERY (Once)                                           │
│  └── Fetch all games from Steam API                        │
│  └── Get player counts for each                            │
│  └── Assign priorities based on activity                   │
│                                                             │
│  REFRESH (Daily)                                            │
│  └── Detect new games only                                 │
│  └── Get player counts for new games                       │
│  └── Add to catalog                                        │
│                                                             │
│  PRIORITY UPDATE (Daily, post-ingestion)                   │
│  └── Use fresh player counts from Bronze                   │
│  └── Recalculate priorities                                │
│  └── Games can move between priorities                     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Priority Thresholds

| Priority | Player Count | Sync Frequency | Est. Games |
|----------|--------------|----------------|------------|
| HIGH | ≥1,000 | Daily | ~2,000 |
| MEDIUM | ≥100 | Weekly | ~8,000 |
| LOW | ≥1 | Monthly | ~40,000 |
| SKIP | 0 | Never | ~100,000 |

### Staleness Check

Before syncing, check `last_synced_at`:
```python
# HIGH: stale after 1 day
# MEDIUM: stale after 7 days
# LOW: stale after 30 days

if last_synced_at < (now - stale_threshold):
    sync_game()
```

---

## API Strategy

### Why Official APIs Only?

We chose to use only official Steam APIs:

| Consideration | Decision |
|---------------|----------|
| Reliability | Official APIs are stable |
| Rate Limits | Well-documented limits |
| Data Quality | Authoritative source |
| Compliance | No ToS violations |

### APIs Used

| API | Endpoint | Purpose | Auth |
|-----|----------|---------|------|
| IStoreService | GetAppList/v1 | Game catalog | API Key |
| Store API | appdetails | Game metadata | None |
| Store API | appreviews | Review stats | None |
| ISteamUserStats | GetNumberOfCurrentPlayers | Player counts | API Key |

### Rate Limiting
```python
# Token bucket algorithm
# 10 requests per second per endpoint

class RateLimiter:
    def __init__(self, rate: float = 10.0):
        self.rate = rate
        self.tokens = rate
        self.last_update = time.time()

    async def acquire(self):
        # Refill tokens based on elapsed time
        # Wait if no tokens available
```

---

## Error Handling

### Retry Strategy
```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, max=30),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError))
)
async def fetch_with_retry():
    ...
```

### Graceful Degradation

If an API call fails after retries:
1. Log the error with context
2. Continue processing other games
3. Report partial success
4. Retry failed games in next run

---

## Performance Considerations

### Concurrency
```python
# Controlled concurrency with semaphore
semaphore = asyncio.Semaphore(5)  # 5 concurrent requests

async def fetch_with_semaphore():
    async with semaphore:
        await fetch_data()
```

### Batch Processing
```python
# Process in batches of 100 for progress reporting
for chunk in chunks(app_ids, size=100):
    results = await process_batch(chunk)
    report_progress(completed, total)
```

### Delta Lake Optimization

- Z-ordering on frequently filtered columns
- Partition pruning with date partitions
- OPTIMIZE and VACUUM in maintenance jobs

---

## Security

### Credentials Management

| Environment | Method |
|-------------|--------|
| Local | Environment variables |
| Fabric (dev) | Notebook variables |
| Fabric (prod) | Key Vault / Secrets |

### Service Principal

Required permissions:
- Storage Blob Data Contributor (OneLake)
- Fabric Workspace access
