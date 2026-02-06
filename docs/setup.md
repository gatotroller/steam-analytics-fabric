# Setup Guide

Complete guide to set up the Steam Analytics Platform.

---

## Prerequisites

- Microsoft Fabric workspace (F64 or higher recommended)
- Steam API Key
- Azure Service Principal (for local development)

---

## Step 1: Get Steam API Key

1. Go to [Steam API Key Registration](https://steamcommunity.com/dev/apikey)
2. Log in with your Steam account
3. Register a new API key (domain name can be "localhost" for testing)
4. Save the key securely

---

## Step 2: Create Fabric Resources

### 2.1 Create Lakehouses

Create 3 lakehouses in your Fabric workspace:

| Lakehouse | Purpose |
|-----------|---------|
| `lh_bronze` | Raw data storage |
| `lh_silver` | Cleaned, transformed data |
| `lh_gold` | Aggregated business data |

**Steps:**
1. Go to your Fabric workspace
2. Click **New** → **Lakehouse**
3. Name it `lh_bronze`
4. Repeat for `lh_silver` and `lh_gold`

### 2.2 Create Fabric Environment

Create a custom environment with required Python libraries:

1. In workspace, click **New** → **Environment**
2. Name: `env_steam_analytics`
3. Go to **Public libraries** tab
4. Add the following packages:
```
azure-identity==1.25.1
azure-storage-file-datalake==12.23.0
httpx==0.28.1
pydantic==2.12.5
pydantic-settings==2.12.0
structlog==25.5.0
tenacity==9.1.2
```

5. Click **Publish** and wait for environment to build (~5 minutes)

### 2.3 Upload Python Package

1. Open `lh_bronze` lakehouse
2. Navigate to **Files** section
3. Create folder structure: `Files/src/steam_analytics/`
4. Upload the entire `src/steam_analytics/` folder
```
lh_bronze/
└── Files/
    └── src/
        └── steam_analytics/
            ├── __init__.py
            ├── config.py
            ├── catalog/
            ├── ingestion/
            └── transformation/
```

### 2.4 Import Notebooks

1. In workspace, click **New** → **Import notebook**
2. Import all notebooks from the `notebooks/` folder:
   - `00_catalog_discovery.ipynb`
   - `00_catalog_refresh.ipynb`
   - `00_monitor.ipynb`
   - `01_bronze_ingestion.ipynb`
   - `02_silver_transform.ipynb`
   - `03_gold_transform.ipynb`

3. Attach environment and lakehouse to each notebook:
   - Environment: `env_steam_analytics`
   - Default lakehouse: `lh_bronze`

---

## Step 3: Configure Notebooks

Update environment variables in each notebook's first cell:
```python
import os

os.environ["STEAM__API_KEY"] = "your_steam_api_key"
os.environ["FABRIC__WORKSPACE_ID"] = "your_workspace_id"
os.environ["FABRIC__BRONZE_LAKEHOUSE_ID"] = "your_bronze_lakehouse_id"
os.environ["FABRIC__SILVER_LAKEHOUSE_ID"] = "your_silver_lakehouse_id"
os.environ["FABRIC__GOLD_LAKEHOUSE_ID"] = "your_gold_lakehouse_id"

# Only for Bronze ingestion (OneLake writes)
os.environ["AZURE_TENANT_ID"] = "your_tenant_id"
os.environ["AZURE_CLIENT_ID"] = "your_client_id"
os.environ["AZURE_CLIENT_SECRET"] = "your_client_secret"
```

### Finding Lakehouse IDs

1. Open any lakehouse
2. Look at the URL: `https://app.fabric.microsoft.com/groups/{workspace_id}/lakehouses/{lakehouse_id}`
3. Copy the `workspace_id` and `lakehouse_id` from the URL

---

## Step 4: Initial Data Load

### 4.1 Run Catalog Discovery (One-time)

This populates the game catalog with ~150,000 games. Takes 1-2 hours.

1. Open `00_catalog_discovery.ipynb`
2. Run all cells
3. Verify catalog created: `lh_bronze/Tables/game_catalog`

### 4.2 Run Full Pipeline

Execute notebooks in order:
```
1. 00_catalog_refresh.ipynb    → Detect new games
2. 01_bronze_ingestion.ipynb   → Extract from Steam APIs
3. 02_silver_transform.ipynb   → Transform to Silver
4. 03_gold_transform.ipynb     → Aggregate to Gold
```

### 4.3 Verify Data

Run `00_monitor.ipynb` to check:
```
PIPELINE HEALTH CHECK
============================================================

BRONZE LAYER
  ✅ game_catalog: 150,000+ records
  ✅ raw_steam_store: X records
  ✅ raw_steam_reviews: X records
  ✅ raw_steam_player_stats: X records

SILVER LAYER
  ✅ dim_games: X records
  ✅ dim_game_reviews: X records
  ✅ fact_player_counts: X records

GOLD LAYER
  ✅ agg_game_metrics: X records
  ✅ agg_price_history: X records
  ✅ agg_genre_summary: X records
```

---

## Step 5: Create Data Pipeline

### 5.1 Create Pipeline

1. In workspace, click **New** → **Data Pipeline**
2. Name: `pl_steam_analytics_daily`

### 5.2 Add Activities

Add 4 Notebook activities in sequence:
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Catalog_Refresh │───►│ Bronze_Ingestion│───►│ Silver_Transform│───►│ Gold_Transform  │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

| Activity | Notebook | Timeout |
|----------|----------|---------|
| Catalog_Refresh | 00_catalog_refresh | 30 min |
| Bronze_Ingestion | 01_bronze_ingestion | 120 min |
| Silver_Transform | 02_silver_transform | 30 min |
| Gold_Transform | 03_gold_transform | 30 min |

### 5.3 Configure Dependencies

For each activity (except first):
- Add dependency: **On Success** from previous activity

### 5.4 Schedule Pipeline

1. Click **Schedule** in pipeline toolbar
2. Configure:
   - Frequency: Daily
   - Time: 10:10 PT
   - Start date: Today

---

## Step 6: Create Power BI Dashboard

### 6.1 Create Semantic Model

1. Open `lh_gold` lakehouse
2. Click **New semantic model**
3. Name: `sm_steam_analytics`
4. Select tables:
   - ☑️ agg_game_metrics
   - ☑️ agg_price_history
   - ☑️ agg_genre_summary

### 6.2 Create Relationships

In semantic model view:
- Drag `agg_game_metrics[app_id]` to `agg_price_history[app_id]`
- Cardinality: One to Many (1:*)

### 6.3 Create Report

1. From semantic model, click **New report**
2. Build 3 pages:
   - Overview
   - Price History
   - Genre Analysis

---

## Troubleshooting

### Common Issues

**"Module not found" error in notebook:**
- Verify `src/` folder is uploaded to `lh_bronze/Files/`
- Check `sys.path.insert(0, "/lakehouse/default/Files")` is in first cell

**API rate limit errors:**
- Reduce concurrency in extractors
- Check Steam API key is valid

**OneLake write failures:**
- Verify Azure credentials are correct
- Check Service Principal has Storage Blob Data Contributor role

**Environment not loading:**
- Wait for environment to finish publishing
- Detach and re-attach environment to notebook

---

## Local Development (Optional)

For testing locally without Fabric:
```bash
# Clone repository
git clone https://github.com/yourusername/steam-analytics.git
cd steam-analytics

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

# Install package
pip install -e ".[dev]"

# Set environment variables
export STEAM__API_KEY="your_key"
# ... other variables

# Run tests
pytest

# Run extraction locally (outputs to local files)
python -m steam_analytics.cli extract --app-ids 730 570 --output ./data
```
