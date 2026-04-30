import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

st.set_page_config(layout="wide", page_title="AI Admin Control Tower", page_icon="🛡️")

st.markdown("""
    <style>
    .main { background-color: #0E1117; }
    div[data-testid="stMetricValue"] { color: #00d4ff; font-weight: 700; }
    .stTabs [data-baseweb="tab-list"] { background-color: transparent; border-bottom: 1px solid #30363d; }
    .stTabs [data-baseweb="tab"] { color: #8b949e !important; font-weight: 600; }
    .stTabs [aria-selected="true"] { border-bottom-color: #00d4ff !important; color: #00d4ff !important; }
    .alert-card { padding: 12px; border-radius: 8px; margin-bottom: 8px; }
    </style>
    """, unsafe_allow_html=True)

session = get_active_session()

@st.cache_data(ttl=3600)
def get_metering_data():
    query = """
    SELECT 
        USAGE_DATE,
        SERVICE_TYPE,
        CREDITS_USED,
        CREDITS_BILLED
    FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
    WHERE USAGE_DATE >= DATEADD('day', -90, CURRENT_DATE())
    ORDER BY USAGE_DATE DESC
    """
    try:
        raw_df = session.sql(query).to_pandas()
        if raw_df.empty:
            return pd.DataFrame()
        raw_df.columns = [c.upper() for c in raw_df.columns]
        raw_df['USAGE_DATE'] = pd.to_datetime(raw_df['USAGE_DATE']).dt.date
        raw_df['CREDITS_USED'] = pd.to_numeric(raw_df['CREDITS_USED']).fillna(0)
        raw_df['CREDITS_BILLED'] = pd.to_numeric(raw_df['CREDITS_BILLED']).fillna(0)
        return raw_df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_warehouse_data():
    query = """
    SELECT 
        DATE_TRUNC('day', START_TIME)::DATE AS USAGE_DATE,
        WAREHOUSE_NAME,
        SUM(CREDITS_USED) AS CREDITS_USED
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
    ORDER BY 1 DESC
    """
    try:
        raw_df = session.sql(query).to_pandas()
        if raw_df.empty:
            return pd.DataFrame()
        raw_df.columns = [c.upper() for c in raw_df.columns]
        raw_df['USAGE_DATE'] = pd.to_datetime(raw_df['USAGE_DATE']).dt.date
        raw_df['CREDITS_USED'] = pd.to_numeric(raw_df['CREDITS_USED']).fillna(0)
        return raw_df
    except Exception as e:
        st.error(f"Warehouse query failed: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_query_data():
    query = """
    SELECT 
        DATE_TRUNC('day', START_TIME)::DATE AS USAGE_DATE,
        USER_NAME,
        WAREHOUSE_NAME,
        QUERY_TYPE,
        CREDITS_USED_CLOUD_SERVICES,
        TOTAL_ELAPSED_TIME,
        QUERY_ID,
        QUERY_TEXT
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    ORDER BY CREDITS_USED_CLOUD_SERVICES DESC NULLS LAST
    LIMIT 5000
    """
    try:
        raw_df = session.sql(query).to_pandas()
        if raw_df.empty:
            return pd.DataFrame()
        raw_df.columns = [c.upper() for c in raw_df.columns]
        raw_df['USAGE_DATE'] = pd.to_datetime(raw_df['USAGE_DATE']).dt.date
        raw_df['CREDITS_USED_CLOUD_SERVICES'] = pd.to_numeric(raw_df['CREDITS_USED_CLOUD_SERVICES']).fillna(0)
        raw_df['TOTAL_ELAPSED_TIME'] = pd.to_numeric(raw_df['TOTAL_ELAPSED_TIME']).fillna(0)
        return raw_df
    except Exception as e:
        st.error(f"Query history failed: {e}")
        return pd.DataFrame()

df = get_metering_data()
wh_df = get_warehouse_data()
q_df = get_query_data()

with st.sidebar:
    st.header("🛡️ Governance Settings")
    budget = st.number_input("Monthly Budget (Credits)", value=50.0, min_value=0.1)
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.subheader("Alert Thresholds")
    alert_warn_pct = st.slider("Warning at (%)", 50, 95, 70)
    alert_anomaly_std = st.slider("Anomaly sensitivity (σ)", 1.0, 4.0, 2.0, 0.5)
    st.divider()
    if not df.empty:
        start_date = st.date_input("Start", df['USAGE_DATE'].min())
        end_date = st.date_input("End", datetime.now().date())
        all_services = sorted(df['SERVICE_TYPE'].unique())
        selected_services = st.multiselect("Service Types", all_services, default=all_services)
    else:
        start_date = datetime.now().date() - timedelta(days=30)
        end_date = datetime.now().date()
        selected_services = []

st.title("🛡️ AI Control Tower: Master View")

if not df.empty:
    f_df = df[(df['USAGE_DATE'] >= start_date) & (df['USAGE_DATE'] <= end_date)].copy()
    if selected_services:
        f_df = f_df[f_df['SERVICE_TYPE'].isin(selected_services)]

    if not f_df.empty:
        total_spent = f_df['CREDITS_BILLED'].sum()
        pct_used = (total_spent / budget) * 100

        if pct_used >= 100:
            st.error(f"🚨 OVER BUDGET by {total_spent - budget:.4f} credits!")
        elif pct_used >= alert_warn_pct:
            st.warning(f"⚠️ Approaching budget limit ({pct_used:.1f}% used)")

        c_gauge, c_mets = st.columns([1.5, 2])
        with c_gauge:
            fig_g = go.Figure(go.Indicator(
                mode="gauge+number", value=total_spent,
                number={'suffix': " Credits", 'valueformat': '.3f'},
                gauge={
                    'axis': {'range': [None, budget * 1.1]},
                    'bar': {'color': "#00d4ff" if pct_used < alert_warn_pct else "#ffaa00"},
                    'steps': [
                        {'range': [0, budget * (alert_warn_pct/100)], 'color': "#1a1c23"},
                        {'range': [budget * (alert_warn_pct/100), budget], 'color': "#ffaa00"},
                        {'range': [budget, budget * 1.1], 'color': "#ff4b4b"}
                    ]
                }
            ))
            fig_g.update_layout(height=250, margin=dict(t=30, b=0), paper_bgcolor='rgba(0,0,0,0)', font={'color': "white"})
            st.plotly_chart(fig_g, use_container_width=True)

        with c_mets:
            st.subheader("Efficiency Metrics")
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Billed", f"{total_spent:.4f}")
            m2.metric("Credits Left", f"{budget - total_spent:.2f}", delta=f"{pct_used:.1f}% Used", delta_color="inverse")
            m3.metric("Service Types", f"{f_df['SERVICE_TYPE'].nunique()}")
            st.write("---")
            days_in_range = (end_date - start_date).days or 1
            m4, m5, m6 = st.columns(3)
            m4.metric("Avg Credits/Day", f"{(total_spent / days_in_range):.4f}")
            ai_credits = f_df[f_df['SERVICE_TYPE'].str.contains('CORTEX', na=False)]['CREDITS_BILLED'].sum()
            m5.metric("AI Credits (Cortex)", f"{ai_credits:.4f}")
            if days_in_range >= 14:
                recent_7 = f_df[f_df['USAGE_DATE'] > (end_date - timedelta(days=7))]['CREDITS_BILLED'].sum()
                prev_7 = f_df[(f_df['USAGE_DATE'] > (end_date - timedelta(days=14))) & (f_df['USAGE_DATE'] <= (end_date - timedelta(days=7)))]['CREDITS_BILLED'].sum()
                wow_change = ((recent_7 - prev_7) / prev_7 * 100) if prev_7 > 0 else 0
                m6.metric("WoW Trend", f"{recent_7:.4f}", delta=f"{wow_change:+.1f}%")

        st.divider()

        st.download_button("📥 Export Filtered Data (CSV)", f_df.to_csv(index=False), "ai_usage_export.csv", "text/csv")

        t_daily, t_service, t_warehouse, t_users, t_queries, t_anomaly, t_forecast = st.tabs([
            "📅 Daily Trends", "⚙️ Service Breakdown", "🏭 Warehouse Costs",
            "👥 Users", "🔍 Top Queries", "🚨 Anomalies", "📈 Forecast"
        ])

        with t_daily:
            c1, c2 = st.columns([2, 1])
            with c1:
                daily_credits = f_df.groupby('USAGE_DATE')['CREDITS_BILLED'].sum().reset_index()
                st.plotly_chart(px.bar(daily_credits, x='USAGE_DATE', y='CREDITS_BILLED', title="Credits Billed Per Day", template="plotly_dark").update_traces(marker_color='#00d4ff'), use_container_width=True)
            with c2:
                service_pie = f_df.groupby('SERVICE_TYPE')['CREDITS_BILLED'].sum().reset_index()
                st.plotly_chart(px.pie(service_pie, values='CREDITS_BILLED', names='SERVICE_TYPE', hole=0.4, title="Cost by Service Type", template="plotly_dark"), use_container_width=True)

        with t_service:
            daily_by_service = f_df.groupby(['USAGE_DATE', 'SERVICE_TYPE'])['CREDITS_BILLED'].sum().reset_index()
            st.plotly_chart(px.area(daily_by_service, x='USAGE_DATE', y='CREDITS_BILLED', color='SERVICE_TYPE', title="Daily Credits by Service Type", template="plotly_dark"), use_container_width=True)
            service_summary = f_df.groupby('SERVICE_TYPE').agg(
                TOTAL_CREDITS=('CREDITS_BILLED', 'sum'),
                DAYS_ACTIVE=('USAGE_DATE', 'nunique')
            ).sort_values('TOTAL_CREDITS', ascending=False).reset_index()
            st.dataframe(service_summary, use_container_width=True)

        with t_warehouse:
            if not wh_df.empty:
                wh_filtered = wh_df[(wh_df['USAGE_DATE'] >= start_date) & (wh_df['USAGE_DATE'] <= end_date)]
                if not wh_filtered.empty:
                    wh_summary = wh_filtered.groupby('WAREHOUSE_NAME')['CREDITS_USED'].sum().sort_values(ascending=False).reset_index()
                    st.plotly_chart(px.bar(wh_summary, x='CREDITS_USED', y='WAREHOUSE_NAME', orientation='h', title="Credits by Warehouse", template="plotly_dark", color='CREDITS_USED', color_continuous_scale='Reds'), use_container_width=True)
                    daily_wh = wh_filtered.groupby(['USAGE_DATE', 'WAREHOUSE_NAME'])['CREDITS_USED'].sum().reset_index()
                    st.plotly_chart(px.area(daily_wh, x='USAGE_DATE', y='CREDITS_USED', color='WAREHOUSE_NAME', title="Daily Warehouse Usage", template="plotly_dark"), use_container_width=True)
                else:
                    st.info("No warehouse data for selected period.")
            else:
                st.info("No warehouse metering data available.")

        with t_users:
            if not q_df.empty:
                user_credits = q_df.groupby('USER_NAME').agg(
                    TOTAL_CREDITS=('CREDITS_USED_CLOUD_SERVICES', 'sum'),
                    QUERY_COUNT=('QUERY_ID', 'count'),
                    AVG_ELAPSED_MS=('TOTAL_ELAPSED_TIME', 'mean')
                ).sort_values('TOTAL_CREDITS', ascending=False).reset_index()
                user_credits['AVG_ELAPSED_SEC'] = (user_credits['AVG_ELAPSED_MS'] / 1000).round(1)

                st.plotly_chart(px.bar(user_credits, x='TOTAL_CREDITS', y='USER_NAME', orientation='h',
                    title="Cloud Service Credits by User", template="plotly_dark",
                    color='QUERY_COUNT', color_continuous_scale='Blues'), use_container_width=True)

                daily_user = q_df.groupby(['USAGE_DATE', 'USER_NAME'])['CREDITS_USED_CLOUD_SERVICES'].sum().reset_index()
                st.plotly_chart(px.area(daily_user, x='USAGE_DATE', y='CREDITS_USED_CLOUD_SERVICES', color='USER_NAME',
                    title="Daily Credits by User", template="plotly_dark"), use_container_width=True)

                st.dataframe(user_credits[['USER_NAME', 'TOTAL_CREDITS', 'QUERY_COUNT', 'AVG_ELAPSED_SEC']], use_container_width=True)
            else:
                st.info("No query history available.")

        with t_queries:
            if not q_df.empty:
                top_queries = q_df.nlargest(20, 'CREDITS_USED_CLOUD_SERVICES')[
                    ['USAGE_DATE', 'USER_NAME', 'WAREHOUSE_NAME', 'QUERY_TYPE', 'CREDITS_USED_CLOUD_SERVICES', 'TOTAL_ELAPSED_TIME', 'QUERY_TEXT']
                ].copy()
                top_queries['ELAPSED_SEC'] = (top_queries['TOTAL_ELAPSED_TIME'] / 1000).round(1)
                top_queries['QUERY_PREVIEW'] = top_queries['QUERY_TEXT'].str[:120]

                st.plotly_chart(px.bar(top_queries.head(10), x='CREDITS_USED_CLOUD_SERVICES', y='QUERY_PREVIEW',
                    orientation='h', title="Top 10 Most Expensive Queries", template="plotly_dark",
                    color='USER_NAME'), use_container_width=True)

                st.dataframe(
                    top_queries[['USAGE_DATE', 'USER_NAME', 'QUERY_TYPE', 'CREDITS_USED_CLOUD_SERVICES', 'ELAPSED_SEC', 'QUERY_PREVIEW']].rename(columns={
                        'CREDITS_USED_CLOUD_SERVICES': 'CREDITS', 'QUERY_PREVIEW': 'QUERY'
                    }),
                    use_container_width=True
                )
            else:
                st.info("No query history available.")

        with t_anomaly:
            daily_total = f_df.groupby('USAGE_DATE')['CREDITS_BILLED'].sum().reset_index().sort_values('USAGE_DATE')
            if len(daily_total) >= 7:
                daily_total['ROLLING_MEAN'] = daily_total['CREDITS_BILLED'].rolling(7, min_periods=3).mean()
                daily_total['ROLLING_STD'] = daily_total['CREDITS_BILLED'].rolling(7, min_periods=3).std()
                daily_total['UPPER_BOUND'] = daily_total['ROLLING_MEAN'] + (alert_anomaly_std * daily_total['ROLLING_STD'])
                daily_total['LOWER_BOUND'] = (daily_total['ROLLING_MEAN'] - (alert_anomaly_std * daily_total['ROLLING_STD'])).clip(lower=0)
                daily_total['IS_ANOMALY'] = daily_total['CREDITS_BILLED'] > daily_total['UPPER_BOUND']

                fig_anom = go.Figure()
                fig_anom.add_trace(go.Scatter(x=daily_total['USAGE_DATE'], y=daily_total['CREDITS_BILLED'],
                    mode='lines+markers', name='Daily Credits', line=dict(color='#00d4ff')))
                fig_anom.add_trace(go.Scatter(x=daily_total['USAGE_DATE'], y=daily_total['ROLLING_MEAN'],
                    mode='lines', name='7-day Avg', line=dict(color='#888', dash='dash')))
                fig_anom.add_trace(go.Scatter(x=daily_total['USAGE_DATE'], y=daily_total['UPPER_BOUND'],
                    mode='lines', name=f'Upper Bound ({alert_anomaly_std}σ)', line=dict(color='#ff4b4b', dash='dot')))

                anomalies = daily_total[daily_total['IS_ANOMALY']]
                if not anomalies.empty:
                    fig_anom.add_trace(go.Scatter(x=anomalies['USAGE_DATE'], y=anomalies['CREDITS_BILLED'],
                        mode='markers', name='Anomaly', marker=dict(color='red', size=12, symbol='x')))

                fig_anom.update_layout(template="plotly_dark", title="Anomaly Detection (Rolling 7-day Window)", height=400)
                st.plotly_chart(fig_anom, use_container_width=True)

                if not anomalies.empty:
                    st.error(f"🚨 {len(anomalies)} anomalous day(s) detected!")
                    st.dataframe(anomalies[['USAGE_DATE', 'CREDITS_BILLED', 'ROLLING_MEAN', 'UPPER_BOUND']].rename(columns={
                        'CREDITS_BILLED': 'ACTUAL', 'ROLLING_MEAN': 'EXPECTED', 'UPPER_BOUND': 'THRESHOLD'
                    }), use_container_width=True)
                else:
                    st.success("✅ No anomalies detected in the selected period.")
            else:
                st.info("Need at least 7 days of data for anomaly detection.")

        with t_forecast:
            daily_total_fc = f_df.groupby('USAGE_DATE')['CREDITS_BILLED'].sum().reset_index().sort_values('USAGE_DATE')
            if len(daily_total_fc) >= 7:
                recent_days = daily_total_fc.tail(14)
                avg_daily = recent_days['CREDITS_BILLED'].mean()
                std_daily = recent_days['CREDITS_BILLED'].std()

                today = datetime.now().date()
                days_in_month = (today.replace(month=today.month % 12 + 1, day=1) - timedelta(days=1)).day
                days_left = days_in_month - today.day
                month_so_far = f_df[f_df['USAGE_DATE'] >= today.replace(day=1)]['CREDITS_BILLED'].sum()
                projected_month = month_so_far + (avg_daily * days_left)
                projected_high = month_so_far + ((avg_daily + std_daily) * days_left)
                projected_low = month_so_far + (max(0, avg_daily - std_daily) * days_left)

                fc1, fc2, fc3 = st.columns(3)
                fc1.metric("Month-to-Date", f"{month_so_far:.4f}")
                fc2.metric("Projected End-of-Month", f"{projected_month:.4f}",
                    delta=f"{'OVER' if projected_month > budget else 'UNDER'} budget by {abs(projected_month - budget):.2f}")
                fc3.metric("Daily Burn Rate", f"{avg_daily:.4f}")

                future_dates = [today + timedelta(days=i) for i in range(1, days_left + 1)]
                forecast_df = pd.DataFrame({
                    'USAGE_DATE': list(daily_total_fc['USAGE_DATE']) + future_dates,
                    'CREDITS': list(daily_total_fc['CREDITS_BILLED']) + [avg_daily] * days_left,
                    'TYPE': ['Actual'] * len(daily_total_fc) + ['Forecast'] * days_left
                })

                fig_fc = go.Figure()
                actual = forecast_df[forecast_df['TYPE'] == 'Actual']
                projected = forecast_df[forecast_df['TYPE'] == 'Forecast']
                fig_fc.add_trace(go.Scatter(x=actual['USAGE_DATE'], y=actual['CREDITS'],
                    mode='lines', name='Actual', line=dict(color='#00d4ff')))
                fig_fc.add_trace(go.Scatter(x=projected['USAGE_DATE'], y=projected['CREDITS'],
                    mode='lines', name='Forecast', line=dict(color='#ffaa00', dash='dash')))
                fig_fc.add_hline(y=budget / days_in_month, line_dash="dot", line_color="red",
                    annotation_text="Daily Budget Target")
                fig_fc.update_layout(template="plotly_dark", title="Spend Forecast (Based on 14-day Avg)", height=400)
                st.plotly_chart(fig_fc, use_container_width=True)

                st.write("**Projection Range (End of Month)**")
                range_df = pd.DataFrame({
                    'Scenario': ['Optimistic', 'Expected', 'Pessimistic'],
                    'Projected Credits': [projected_low, projected_month, projected_high],
                    'vs Budget': [projected_low - budget, projected_month - budget, projected_high - budget]
                })
                st.dataframe(range_df, use_container_width=True)
            else:
                st.info("Need at least 7 days of data for forecasting.")

    else:
        st.warning("No data for current filters.")
else:
    st.info("No metering data available.")
