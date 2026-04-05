import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta

# 1. Page Config
st.set_page_config(layout="wide", page_title="AI Admin Control Tower", page_icon="🛡️")

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

# 2. Master Data Fetch
@st.cache_data(ttl=3600)
def get_comprehensive_data():
    view_cortex = "SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY"
    view_queries = "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY"
    
    query = f"""
    SELECT 
        u.START_TIME, 
        u.FUNCTION_NAME, 
        u.MODEL_NAME, 
        u.CREDITS,
        q.USER_NAME,
        u.QUERY_ID
    FROM {view_cortex} u
    LEFT JOIN {view_queries} q ON u.QUERY_ID = q.QUERY_ID
    ORDER BY u.START_TIME DESC LIMIT 20000
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

df = get_comprehensive_data()

# 3. Sidebar
with st.sidebar:
    st.header("🛡️ Governance Settings")
    budget = st.number_input("Monthly Budget", value=10.0, min_value=0.1)
    st.divider()
    if not df.empty:
        start_date = st.date_input("Start", df['USAGE_DATE'].min())
        end_date = st.date_input("End", datetime.now().date())
        selected_users = st.multiselect("Users", sorted(df['USER'].unique()), default=df['USER'].unique())

# 4. Main Dashboard
st.title("🛡️ AI Control Tower: Master View")

if not df.empty:
    f_df = df[(df['USAGE_DATE'] >= start_date) & (df['USAGE_DATE'] <= end_date)]
    if selected_users: f_df = f_df[f_df['USER'].isin(selected_users)]

    if not f_df.empty:
        total_spent = f_df['CREDITS'].sum()
        pct_used = (total_spent / budget) * 100
        
        # --- Top KPIs ---
        c_gauge, c_mets = st.columns([1.5, 2])
        with c_gauge:
            fig_g = go.Figure(go.Indicator(
                mode = "gauge+number", value = total_spent,
                number = {'suffix': " Credits", 'valueformat':'.3f'},
                gauge = {
                    'axis': {'range': [None, budget * 1.1]},
                    'bar': {'color': "#00d4ff" if pct_used < 70 else "#ffaa00"},
                    'steps': [
                        {'range': [0, budget*0.7], 'color': "#1a1c23"},
                        {'range': [budget*0.7, budget], 'color': "#ffaa00"},
                        {'range': [budget, budget*1.1], 'color': "#ff4b4b"}
                    ]
                }
            ))
            fig_g.update_layout(height=250, margin=dict(t=30, b=0), paper_bgcolor='rgba(0,0,0,0)', font={'color': "white"})
            st.plotly_chart(fig_g, use_container_width=True)

        with c_mets:
            st.subheader("Efficiency Metrics")
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Credits", f"{total_spent:.4f}")
            m2.metric("Credits Left", f"{budget-total_spent:.2f}", delta=f"{pct_used:.1f}% Used", delta_color="inverse")
            m3.metric("Active Users", f"{f_df['USER'].nunique()}")
            st.write("---")
            m4, m5 = st.columns(2)
            m4.metric("Avg Credits/User", f"{(total_spent/f_df['USER'].nunique()):.4f}")
            m5.metric("Avg Credits/Day", f"{(total_spent/((end_date-start_date).days or 1)):.4f}")

        st.divider()

        # --- Enhanced Tabs ---
        t_daily, t_user, t_func = st.tabs(["📅 Daily & Service Trends", "👥 User Consumption", "⚙️ Top Functions"])
        
        with t_daily:
            c1, c2 = st.columns([2, 1])
            with c1:
                daily_credits = f_df.groupby('USAGE_DATE')['CREDITS'].sum().reset_index()
                st.plotly_chart(px.bar(daily_credits, x='USAGE_DATE', y='CREDITS', title="Credits Consumed Per Day", template="plotly_dark").update_traces(marker_color='#00d4ff'), use_container_width=True)
            with c2:
                st.plotly_chart(px.pie(f_df, values='CREDITS', names='SERVICE', hole=0.4, title="Service Type Distribution", template="plotly_dark"), use_container_width=True)

        with t_user:
            user_spend = f_df.groupby('USER')['CREDITS'].sum().sort_values(ascending=False).reset_index()
            st.plotly_chart(px.bar(user_spend, x='CREDITS', y='USER', orientation='h', title="Credits Consumed Per User", template="plotly_dark", color='CREDITS', color_continuous_scale='Reds'), use_container_width=True)

        with t_func:
            # Top 10 Functions (Service + Model combo)
            f_df['FUNC_COMB'] = f_df['SERVICE'] + " (" + f_df['MODEL'] + ")"
            top_funcs = f_df.groupby('FUNC_COMB')['CREDITS'].sum().sort_values(ascending=False).head(10).reset_index()
            st.plotly_chart(px.bar(top_funcs, x='CREDITS', y='FUNC_COMB', orientation='h', title="Top 10 Functions by Credit Spend", template="plotly_dark", color_discrete_sequence=['#ffaa00']), use_container_width=True)
            st.dataframe(f_df.sort_values('CREDITS', ascending=False).head(15)[['USAGE_DATE', 'USER', 'SERVICE', 'MODEL', 'CREDITS']], use_container_width=True)

    else:
        st.warning("No data for current filters.")
else:
    st.info("No AI usage detected.")