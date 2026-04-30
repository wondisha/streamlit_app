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
