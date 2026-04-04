import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta

# 1. Page Config
st.set_page_config(layout="wide", page_title="AI Control Tower", page_icon="🛡️")

st.markdown("""
    <style>
    .main { background-color: #0E1117; }
    div[data-testid="stMetricValue"] { color: #00d4ff; font-weight: 700; }
    .stTabs [data-baseweb="tab-list"] { background-color: transparent; border-bottom: 1px solid #30363d; }
    .stTabs [data-baseweb="tab"] { color: #8b949e !important; font-weight: 600; }
    .stTabs [aria-selected="true"] { border-bottom-color: #00d4ff !important; color: #00d4ff !important; }
    </style>
    """, unsafe_allow_html=True)

session = get_active_session()

# 2. Data Fetching
@st.cache_data(ttl=3600)
def get_master_data():
    view_cortex = "SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY"
    view_queries = "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"
    query = f"""
    SELECT u.START_TIME, u.FUNCTION_NAME, u.MODEL_NAME, u.CREDITS, q.USER_NAME
    FROM {view_cortex} u
    LEFT JOIN {view_queries} q ON u.QUERY_ID = q.QUERY_ID
    ORDER BY u.START_TIME DESC LIMIT 15000
    """
    try:
        raw_df = session.sql(query).to_pandas()
        if raw_df.empty: return pd.DataFrame()
        raw_df.columns = [c.upper() for c in raw_df.columns]
        df = pd.DataFrame({
            'USAGE_DATE': pd.to_datetime(raw_df['START_TIME']).dt.date,
            'SERVICE': raw_df['FUNCTION_NAME'],
            'MODEL': raw_df['MODEL_NAME'].fillna('N/A'),
            'CREDITS': pd.to_numeric(raw_df['CREDITS']).fillna(0),
            'USER': raw_df['USER_NAME'].fillna('System/Service'),
            'INVOCATIONS': 1
        })
        return df
    except: return pd.DataFrame()

df = get_master_data()

# 3. Sidebar
with st.sidebar:
    st.header("🛡️ Governance Settings")
    budget = st.number_input("Monthly Budget", value=10.0, min_value=0.1)
    st.divider()
    if not df.empty:
        start_date = st.date_input("Start", df['USAGE_DATE'].min())
        end_date = st.date_input("End", datetime.now().date())
        u_list = sorted(df['USER'].unique().tolist())
        selected_users = st.multiselect("Team Members", u_list, default=u_list)

# 4. Main UI
st.title("🛡️ AI Control Tower: Master View")

if not df.empty:
    f_df = df[(df['USAGE_DATE'] >= start_date) & (df['USAGE_DATE'] <= end_date)]
    if selected_users: f_df = f_df[f_df['USER'].isin(selected_users)]

    if not f_df.empty:
        total_spent = f_df['CREDITS'].sum()
        pct_used = (total_spent / budget) * 100
        
        # --- TRAFFIC LIGHT GAUGE ---
        c_gauge, c_mets = st.columns([1.5, 2])
        
        with c_gauge:
            # Determine Indicator Color
            status_color = "#00d4ff" # Blue (Safe)
            if pct_used >= 90: status_color = "#ff4b4b" # Red (Critical)
            elif pct_used >= 70: status_color = "#ffaa00" # Orange (Warning)

            fig_g = go.Figure(go.Indicator(
                mode = "gauge+number",
                value = total_spent,
                number = {'suffix': " Credits", 'font': {'size': 35}, 'valueformat':'.3f'},
                gauge = {
                    'axis': {'range': [None, budget * 1.1], 'tickwidth': 1, 'tickcolor': "white"},
                    'bar': {'color': status_color},
                    'bgcolor': "white",
                    'borderwidth': 2,
                    'bordercolor': "gray",
                    'steps': [
                        {'range': [0, budget * 0.7], 'color': "#1a1c23"},      # Dark: Safe
                        {'range': [budget * 0.7, budget], 'color': "#ffaa00"}, # Orange: Warning
                        {'range': [budget, budget * 1.1], 'color': "#ff4b4b"}  # Red: Critical
                    ],
                    'threshold': {
                        'line': {'color': "white", 'width': 4},
                        'thickness': 0.75,
                        'value': budget
                    }
                }
            ))
            fig_g.update_layout(height=280, margin=dict(t=50, b=0, l=25, r=25), paper_bgcolor='rgba(0,0,0,0)', font={'color': "white"})
            st.plotly_chart(fig_g, use_container_width=True)
            
            # Traffic Light Legend Label
            st.markdown(f"""
                <div style="text-align:center; margin-top:-20px;">
                    <span style="color:#00d4ff">●</span> Safe (<70%) &nbsp; 
                    <span style="color:#ffaa00">●</span> Warning (70-100%) &nbsp; 
                    <span style="color:#ff4b4b">●</span> Critical (>100%)
                </div>
            """, unsafe_allow_html=True)

        with c_mets:
            st.subheader("Account Performance")
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Credits", f"{total_spent:.4f}")
            m2.metric("Credits Left", f"{budget-total_spent:.2f}", delta=f"{pct_used:.1f}% Used", delta_color="inverse")
            m3.metric("Active Users", f"{f_df['USER'].nunique()}")
            st.write("---")
            m4, m5 = st.columns(2)
            m4.metric("Avg Cost / Call", f"{(total_spent/len(f_df)):.6f}")
            days = (f_df['USAGE_DATE'].max() - f_df['USAGE_DATE'].min()).days or 1
            m5.metric("Daily Burn Rate", f"{(total_spent/days):.4f}")

        st.divider()

        # Tabs
        t_trend, t_team, t_shame = st.tabs(["📊 Trends", "👥 Team Consumption", "🚩 Hall of Shame"])
        with t_trend:
            col_a, col_b = st.columns([2, 1])
            with col_a:
                daily = f_df.groupby('USAGE_DATE')['CREDITS'].sum().reset_index()
                st.plotly_chart(px.line(daily, x='USAGE_DATE', y='CREDITS', template="plotly_dark", title="Daily Spend").update_traces(line_color='#00d4ff'), use_container_width=True)
            with col_b:
                st.plotly_chart(px.sunburst(f_df, path=['SERVICE', 'MODEL'], values='CREDITS', template="plotly_dark", title="Cost Hierarchy"), use_container_width=True)

        with t_team:
            user_data = f_df.groupby('USER')['CREDITS'].sum().sort_values(ascending=False).reset_index()
            st.plotly_chart(px.bar(user_data.head(15), x='CREDITS', y='USER', orientation='h', template="plotly_dark", title="User Spend Leaderboard", color='CREDITS', color_continuous_scale='Reds'), use_container_width=True)

        with t_shame:
            st.dataframe(f_df.sort_values('CREDITS', ascending=False).head(15)[['USAGE_DATE', 'USER', 'SERVICE', 'MODEL', 'CREDITS']], use_container_width=True)
else:
    st.info("No data detected.")