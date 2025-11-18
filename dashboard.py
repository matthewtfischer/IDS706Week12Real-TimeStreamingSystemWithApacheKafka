import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Real-Time Ride-Sharing Dashboard", layout="wide")
st.title("Real-Time Ride-Sharing Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"

@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)

engine = get_engine(DATABASE_URL)

def load_data(status_filter: str | None = None, limit: int = 500) -> pd.DataFrame:
    base_query = "SELECT * FROM trips"
    params = {}
    if status_filter and status_filter != "All":
        base_query += " WHERE status = :status"
        params["status"] = status_filter
    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params["limit"] = limit

    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(base_query), con=conn, params=params)
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()

# Sidebar controls
status_options = ["All", "completed", "cancelled", "no_show"]
selected_status = st.sidebar.selectbox("Filter by Status", status_options)
update_interval = st.sidebar.slider("Update Interval (seconds)", min_value=2, max_value=20, value=5)
limit_records = st.sidebar.number_input("Number of records to load", min_value=50, max_value=2000, value=500, step=50)

if st.sidebar.button("Refresh now"):
    st.rerun()

placeholder = st.empty()

while True:
    df = load_data(selected_status, limit=int(limit_records))

    with placeholder.container():
        if df.empty:
            st.warning("No records found. Waiting for data...")
            time.sleep(update_interval)
            continue

        # parse timestamps
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        if "start_time" in df.columns:
            df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
        if "end_time" in df.columns:
            df["end_time"] = pd.to_datetime(df["end_time"], errors="coerce")

        total_trips = len(df)
        total_revenue = float(df["price"].sum()) if "price" in df.columns else 0.0
        avg_rating = float(df["rating"].dropna().astype(float).mean()) if "rating" in df.columns else None
        avg_distance = float(df["distance_km"].mean()) if "distance_km" in df.columns else 0.0
        completed = len(df[df["status"] == "completed"])
        cancellation_rate = (len(df[df["status"] == "cancelled"]) / total_trips * 100) if total_trips else 0.0
        avg_duration = (df["duration_seconds"].mean() / 60) if "duration_seconds" in df.columns else 0.0

        st.subheader(f"Displaying {total_trips} trips (Filter: {selected_status})")

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Trips", total_trips)
        k2.metric("Total Revenue", f"${total_revenue:,.2f}")
        k3.metric("Avg Rating", f"{avg_rating:.2f}" if avg_rating else "N/A")
        k4.metric("Avg Distance (km)", f"{avg_distance:.2f}")
        k5.metric("Cancellation Rate", f"{cancellation_rate:.2f}%")

        # Raw data preview
        st.markdown("### Recent Trips (Top 10)")
        st.dataframe(df.head(10), use_container_width=True)

        # Trips by city (count) and revenue by city
        city_count = df.groupby("city").size().reset_index(name="trips").sort_values("trips", ascending=False)
        city_revenue = df.groupby("city")["price"].sum().reset_index().sort_values("price", ascending=False)

        fig1 = px.bar(city_count, x="city", y="trips", title="Trips by City")
        fig2 = px.bar(city_revenue, x="city", y="price", title="Revenue by City")

        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(fig1, use_container_width=True)
        with col2:
            st.plotly_chart(fig2, use_container_width=True)

        # Time series: trips over time
        if "timestamp" in df.columns:
            ts = df.set_index("timestamp").resample("1Min").size().reset_index(name="trips")
            if not ts.empty:
                fig_ts = px.line(ts, x="timestamp", y="trips", title="Trips per Minute (last loaded window)")
                st.plotly_chart(fig_ts, use_container_width=True)

        st.markdown("---")
        st.caption(f"Last updated: {datetime.now().isoformat()} • Auto-refresh: {update_interval}s")

    time.sleep(update_interval)
