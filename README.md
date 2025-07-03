# Data Engineering Overview

This repository contains the core data pipelines that power our machine learning research and product initiatives. The data is processed through two distinct, purpose-built pipelines.

---

## 1. The Research Pipeline

*   **Purpose:** To create a methodologically sound, survivorship-bias-free dataset for rigorously backtesting quantitative trading strategies. This is the foundation for all our alpha research.
*   **Scripts:** `create_universe.py` & `main_pipeline.py`
*   **Methodology:** This pipeline acts as a "historian," reconstructing the Top 200 crypto universe as it existed at each point in time. It uses this dynamic universe to fetch data, ensuring our models are not biased by only looking at today's successful assets.
*   **Output:** `crypto_market_data.parquet`

---

## 2. The Dashboard Pipeline

*   **Purpose:** To provide a comprehensive and up-to-date dataset for our user-facing SaaS dashboard.
*   **Script:** `create_dashboard_data.py`
*   **Methodology:** This pipeline is simpler. It fetches the current Top 200 assets by market cap and acquires their full historical data. This ensures the dashboard is populated with data for the assets our users recognize and care about today.
*   **Output:** `dashboard_data.parquet`

---

## Shared Infrastructure

Both pipelines are built on the same robust, reusable code foundation and leverage a shared Google Cloud Storage (GCS) cache. This ensures that API calls are minimized across the team and that subsequent runs of either pipeline are fast and efficient.

> **Note:** For detailed setup and execution instructions, please refer to the `README.md` file within each respective project folder.

### 📬 Issue Submission Process

To ensure structured and trackable collaboration across teams (ML, LLM, Dashboard), all data-related requests should be submitted as [GitHub Issues](https://github.com/teklystudio/data-engineering/issues) using our standardized issue template.

You can use this for:

- 📊 Requesting new data or metrics  
- ⚙️ Suggesting new features or calculations  
- 🐛 Reporting bugs or inconsistencies  
- ❓ Asking questions related to pipeline usage

> 📌 Navigate to **“New Issue”** in the repo and select the **`🔧 Data Request / Issue`** template to get started.
