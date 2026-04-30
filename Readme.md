

markdown
# Snowflake AI Control Tower: Master View

An advanced Snowflake Native App built with Streamlit to provide Account Admins with a centralized governance dashboard for all Snowflake compute and AI service costs. This tool monitors credit consumption across all service types, detects spending anomalies, forecasts end-of-month spend, and provides granular visibility into per-user and per-query attribution.

## 🚀 Key Features

- **Traffic Light Governance**: A dynamic gauge showing budget utilization with configurable visual zones (Safe, Warning, Critical).
- **User Consumption Leaderboard**: Identifies specific users driving costs by analyzing cloud service credits from Query History.
- **Efficiency Metrics**: Real-time calculation of Avg Credits/Day, Week-over-Week trends, and AI-specific (Cortex) spend isolation.
- **Anomaly Detection**: Rolling 7-day statistical monitoring with configurable sensitivity (σ) to flag sudden spikes in spend.
- **Spend Forecasting**: Projects end-of-month credit consumption with optimistic, expected, and pessimistic scenarios based on 14-day rolling averages.
- **Top Queries Analysis**: Surfaces the most expensive individual queries with SQL previews, user attribution, and elapsed time.
- **Warehouse Breakdown**: Per-warehouse credit trends to identify idle or over-provisioned resources.
- **Service Type Decomposition**: Stacked area charts showing cost evolution across Warehouse Metering, Cortex Code, Auto-Clustering, and other service types.

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Engine | Snowflake Account Usage Views |
| Frontend | Streamlit-in-Snowflake |
| Data Layer | Snowpark Python |
| Visualization | Plotly (Gauge, Area, Bar, Scatter) |
| Analysis | Pandas, NumPy (rolling stats, forecasting) |

## 📋 Prerequisites & Setup

### 1. Permissions

The role executing this app must have:

```sql
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;
This provides access to:

SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
2. Deployment
sql
CREATE STREAMLIT my_db.my_schema.ai_control_tower
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = 'COMPUTE_WH';
3. Required Packages
All available in Snowflake's Anaconda channel — no external internet access required:

streamlit
plotly
pandas
numpy
📊 How It Works
The app uses three cached data pipelines (TTL: 1 hour) to minimize its own compute footprint:

sql
-- 1. Daily metering across all service types (90-day window)
SELECT USAGE_DATE, SERVICE_TYPE, CREDITS_USED, CREDITS_BILLED
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE USAGE_DATE >= DATEADD('day', -90, CURRENT_DATE());

-- 2. Per-warehouse granularity
SELECT DATE_TRUNC('day', START_TIME)::DATE, WAREHOUSE_NAME, SUM(CREDITS_USED)
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('day', -90, CURRENT_TIMESTAMP())
GROUP BY 1, 2;

-- 3. Per-query attribution (30-day window, top 5000 by cost)
SELECT USER_NAME, QUERY_TYPE, CREDITS_USED_CLOUD_SERVICES, TOTAL_ELAPSED_TIME, QUERY_TEXT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY CREDITS_USED_CLOUD_SERVICES DESC LIMIT 5000;
⚙️ Configuration
All settings are controlled via the sidebar:

Setting	Default	Description
Monthly Budget	50 credits	Target for gauge and forecast comparison
Warning Threshold	70%	Alert fires when spend exceeds this %
Anomaly Sensitivity	2.0σ	Std deviations above rolling mean to flag
Date Range	Full 90-day window	Filters all tabs
Service Types	All	Filter to specific services
📸 Dashboard Tabs
Tab	Description
📅 Daily Trends	Bar chart + pie chart of daily spend by service
⚙️ Service Breakdown	Stacked area chart + summary table per service type
🏭 Warehouse Costs	Per-warehouse ranking and daily trends
👥 Users	Per-user credit attribution, query counts, avg latency
🔍 Top Queries	20 most expensive queries with SQL preview
🚨 Anomalies	Statistical spike detection with visual markers
📈 Forecast	End-of-month projection with confidence range
🔮 Future Enhancements
Slack/email notifications on anomaly or budget breach
Integration with Snowflake Budgets API for automated enforcement
Role-based views for team leads (see only their team's usage)
Cortex AI function-level tracking (per-model attribution) when account usage data becomes available
📄 License
Copyright (c) 2026 Wondi Wolde. All rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## 📊 How it Works
The app uses a **Master Fetch** query that joins AI usage logs with general query metadata to resolve internal IDs into readable names.

```sql
SELECT 
    u.START_TIME, 
    u.MODEL_NAME, 
    u.CREDITS, 
    q.USER_NAME
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY u
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q 
  ON u.QUERY_ID = q.QUERY_ID;
