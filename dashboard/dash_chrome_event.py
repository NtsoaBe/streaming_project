import streamlit as st
import pandas as pd
import plotly.express as px
from pinotdb import connect
import time

# =========================
# Streamlit configuration
# =========================

st.set_page_config(
    page_title="Chrome Event Monitor",
    page_icon="🌐",
    layout="wide"
)

st.title("🌐 Real-time Chrome Event Dashboard")


# =========================
# Query Data from Pinot
# =========================

@st.cache_data(ttl=5)  # Refresh cache every 5 seconds for real-time updates
def fetch_pinot_data():
    """Connect to Pinot Broker and query chrome_event metrics using category mapping."""
    conn = connect(host='localhost', port=8099, path='/query/sql', scheme='http')
    
    # Query updated to fetch BOTH TRANCHE_30MIN and TRANCHE_5MIN
    query = """
    select 
        count(*) as cnt,
        TRANCHE_30MIN,
        TRANCHE_5MIN,
        PAGENAME as category
    FROM chrome_event
    GROUP BY
        TRANCHE_30MIN,
        TRANCHE_5MIN,
        PAGENAME
    ORDER BY TRANCHE_5MIN ASC
    """
    
    curs = conn.cursor()
    curs.execute(query)
    
    # Extract results and column headers
    rows = curs.fetchall()
    cols = [desc[0] for desc in curs.description]
    
    conn.close()
    
    return pd.DataFrame(rows, columns=cols)


# Fetch data
try:
    df = fetch_pinot_data()
except Exception as e:
    st.error(f"Failed to connect to Pinot Broker: {e}")
    st.stop()


# Ensure correct data type for event count
if not df.empty:
    df["cnt"] = pd.to_numeric(df["cnt"])


# =========================
# Global statistics
# =========================

total_events = df["cnt"].sum() if not df.empty else 0
unique_tranches = df["TRANCHE_30MIN"].nunique() if not df.empty else 0
unique_categories = df["category"].nunique() if not df.empty else 0


# =========================
# Score Cards
# =========================

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="Total Chrome Events",
        value=f"{total_events:,}"
    )

with col2:
    st.metric(
        label="30-Min Time Tranches",
        value=unique_tranches
    )

with col3:
    st.metric(
        label="Unique Categories",
        value=unique_categories
    )

st.divider()


# =========================================================
# 1. HISTORICAL BAR CHART (30-Minute Granularity)
# =========================================================

st.subheader("📊 Event Count per 30-Minute Interval by Category")

if not df.empty:
    # Re-aggregate counts for 30-min display because df contains 5-min rows
    df_30min = df.groupby(["TRANCHE_30MIN", "category"], as_index=False)["cnt"].sum()
    df_30min = df_30min.sort_values("TRANCHE_30MIN")

    fig_bar = px.bar(
        df_30min,
        x="TRANCHE_30MIN",
        y="cnt",
        color="category",
        barmode="group",
        text="cnt"
    )

    fig_bar.update_traces(
        texttemplate="%{text:,}",
        textposition="outside"
    )

    fig_bar.update_layout(
        xaxis_title="30-Min Tranche (HH:mm)",
        yaxis_title="Total Events",
        legend_title="Category",
        template="plotly_white",
        height=400
    )

    st.plotly_chart(fig_bar, use_container_width=True)
else:
    st.info("No data returned from Pinot table chrome_event.")


# =========================================================
# 2. REAL-TIME TREND LINE CHART (5-Minute Granularity)
# =========================================================

st.subheader("📈 Chrome Event Ingestion Trend by Category (5-Min Interval)")

if not df.empty:
    fig_line = px.line(
        df,
        x="TRANCHE_5MIN",
        y="cnt",
        color="category",
        markers=True
    )

    fig_line.update_traces(line=dict(width=3))

    fig_line.update_layout(
        xaxis_title="5-Min Tranche",
        yaxis_title="Event Volume",
        legend_title="Category",
        hovermode="x unified",
        template="plotly_white",
        height=450
    )

    st.plotly_chart(fig_line, use_container_width=True)


# =========================================================
# Auto-Refresh Control
# =========================================================

st.divider()
st.sidebar.header("Auto Refresh Settings")
refresh_rate = st.sidebar.slider("Refresh interval (seconds)", min_value=2, max_value=60, value=5)

# Trigger auto-reload for real-time stream tracking
st.sidebar.write(f"Refreshing every {refresh_rate} seconds...")
time.sleep(refresh_rate)
st.rerun()