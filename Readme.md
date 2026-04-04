Snowflake AI Control Tower: Master View
An advanced Snowflake Native App built with Streamlit to provide Account Admins with a centralized governance dashboard for Cortex AI services. This tool monitors credit consumption, detects spending anomalies, and provides granular visibility into team usage.

🚀 Key Features
Traffic Light Governance: A dynamic gauge showing budget utilization with clear visual zones (Safe, Warning, Critical).

Team Consumption Leaderboard: Identifies specific users (e.g., SFUSERAI, SYSTEM) driving AI costs by joining Cortex telemetry with Snowflake Query History.

Efficiency Metrics: Real-time calculation of Credits per Call and Daily Burn Rate to monitor prompt efficiency.

Anomaly Detection: Statistical monitoring to flag sudden spikes in AI spend.

Model Hierarchy: Sunburst visualizations to see which specific LLMs (Llama3, Mistral, etc.) are consuming the most credits.

🛠️ Tech Stack
Engine: Snowflake Cortex AI

Frontend: Streamlit

Data Layer: Snowpark Python

Visualization: Plotly (Gauge, Sunburst, Trend Lines)

📋 Prerequisites & Setup
1. Permissions
To see user-level data, the role executing this app (usually ACCOUNTADMIN) must have:

IMPORTED PRIVILEGES on the SNOWFLAKE shared database.

Access to SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY.

Access to SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY.

2. Deployment
Copy streamlit_app.py into a Snowflake Streamlit object or a Native App project.

Ensure your Warehouse has access to the internet if you are using external Python packages (though this script uses standard Snowflake-included libraries).

📊 How it Works (The Logic)
The app uses a Master Fetch query that joins AI usage logs with general query metadata. This allows the dashboard to resolve internal USER_IDs into readable USER_NAMEs, providing accountability for AI spend across your Snowflake account.

SQL
-- Core Logic Snippet
SELECT u.START_TIME, u.MODEL_NAME, u.CREDITS, q.USER_NAME
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY u
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q 
  ON u.QUERY_ID = q.QUERY_ID
🛡️ Governance Thresholds
Safe Zone (<70%): Normal operation.

Warning Zone (70-100%): Approaching credit quota.

Critical Zone (>100%): Budget exceeded; alert triggered.