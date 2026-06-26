import streamlit as st
import clickhouse_connect
import pandas as pd
import plotly.express as px

# ==========================================
# 1. KONFIGURASI HALAMAN & KONEKSI
# ==========================================
st.set_page_config(
    page_title="Flight Delay Insights",
    layout="wide"
)

st.title("✈️ Executive Flight Delay Insights")
st.sidebar.header("Dashboard Filter")

# Sidebar Slider
selected_year = st.sidebar.slider(
    "Select Year Range",
    min_value=2021,
    max_value=2025,
    value=(2021, 2025)
)

@st.cache_resource
def init_connection():
    return clickhouse_connect.get_client(
        host="13.215.79.3",
        port=8123,
        username="default",
        password="rahasia123",
        database="flight_delay"
    )

client = init_connection()

@st.cache_data(ttl=300)
def load_data(query):
    query = query.strip().rstrip(";")
    return client.query_df(query)

# ==========================================
# 2. EXECUTIVE KPI
# ==========================================
st.markdown(f"### Executive Key Performance Indicators (KPI) | {selected_year[0]}-{selected_year[1]}")
kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

df_total_flights = load_data(f"""
    SELECT SUM(total_flights) AS total_flights
    FROM flight_delay.agg_carrier_performance
    WHERE year BETWEEN {selected_year[0]} AND {selected_year[1]}
""")

df_delayed_flights = load_data(f"""
    SELECT ROUND(SUM(total_flights * arr_delay_rate), 0) AS delayed_flights
    FROM flight_delay.agg_carrier_performance
    WHERE year BETWEEN {selected_year[0]} AND {selected_year[1]}
""")

df_overall_rate = load_data(f"""
    SELECT ROUND(SUM(total_flights * arr_delay_rate) / SUM(total_flights) * 100, 1) AS overall_delay_rate_pct
    FROM flight_delay.agg_carrier_performance
    WHERE year BETWEEN {selected_year[0]} AND {selected_year[1]}
""")

with kpi_col1:
    st.metric("Total Flights", f"{int(df_total_flights['total_flights'][0]):,}")
with kpi_col2:
    st.metric("Delayed Flights", f"{int(df_delayed_flights['delayed_flights'][0]):,}")
with kpi_col3:
    st.metric("Overall Arrival Delay Rate", f"{df_overall_rate['overall_delay_rate_pct'][0]}%")
with kpi_col4:
    st.metric("Data Source", "BTS On-Time Performance")

st.divider()

# ==========================================
# 3. TIME-BASED TRENDS & HEATMAP
# ==========================================
trend_col1, trend_col2 = st.columns(2)

with trend_col1:
    st.subheader("Monthly Delay Trends (Mins)")
    query_monthly = f"""
    SELECT
        makeDate(year, month, 1) AS flight_date,
        ROUND(avg_arr_delay, 2) AS avg_arrival_delay_min,
        ROUND(avg_dep_delay, 2) AS avg_departure_delay_min
    FROM flight_delay.agg_monthly_delay
    WHERE year BETWEEN {selected_year[0]} AND {selected_year[1]}
    ORDER BY flight_date ASC
    """
    df_monthly = load_data(query_monthly)
    if not df_monthly.empty:
        fig_trend = px.line(df_monthly, x="flight_date", y=["avg_arrival_delay_min", "avg_departure_delay_min"], 
                           color_discrete_sequence=["#1f77b4", "#ff7f0e"])
        st.plotly_chart(fig_trend, use_container_width=True)

with trend_col2:
    st.subheader("Flight Delay Risk Heatmap (%)")
    # Filter WHERE year dihapus karena tabel agg_hourly_delay tidak punya kolom year
    query_heatmap = """
        SELECT dep_hour AS departure_hour, day_of_week, round(avg(delay_rate) * 100, 1) AS delay_rate_pct
        FROM flight_delay.agg_hourly_delay
        GROUP BY departure_hour, day_of_week
        ORDER BY departure_hour ASC, day_of_week ASC;
    """
    df_heatmap = load_data(query_heatmap)
    if not df_heatmap.empty:
        fig_heat = px.density_heatmap(df_heatmap, x="departure_hour", y="day_of_week", z="delay_rate_pct", color_continuous_scale="Plasma")
        st.plotly_chart(fig_heat, use_container_width=True)

st.divider()

# ==========================================
# 4. ROOT CAUSE ANALYSIS
# ==========================================
st.markdown("### Delay Root Cause Distribution by Airline")
query_cause = f"""
SELECT airline_code, 
       ROUND(AVG(pct_carrier) * 100, 1) AS carrier_delay_pct, 
       ROUND(AVG(pct_weather) * 100, 1) AS weather_delay_pct, 
       ROUND(AVG(pct_nas) * 100, 1) AS nas_delay_pct, 
       ROUND(AVG(pct_late_aircraft) * 100, 1) AS late_aircraft_pct
FROM flight_delay.agg_delay_reason
WHERE year BETWEEN {selected_year[0]} AND {selected_year[1]}
GROUP BY airline_code
ORDER BY carrier_delay_pct DESC LIMIT 10
"""
df_cause = load_data(query_cause)
if not df_cause.empty:
    fig_cause = px.bar(df_cause, x="airline_code", y=["carrier_delay_pct", "weather_delay_pct", "nas_delay_pct", "late_aircraft_pct"], barmode="stack", color_discrete_sequence=px.colors.qualitative.Pastel)
    st.plotly_chart(fig_cause, use_container_width=True)

st.divider()

# ==========================================
# 5. GEOGRAPHICAL ANALYSIS
# ==========================================
geo_col1, geo_col2 = st.columns(2)
with geo_col1:
    st.subheader("Top Routes by Delay")
    df_route = load_data(f"SELECT route, SUM(total_flights) AS total, AVG(arr_delay_rate) AS rate FROM flight_delay.agg_route_performance WHERE year BETWEEN {selected_year[0]} AND {selected_year[1]} GROUP BY route ORDER BY total DESC LIMIT 10")
    st.dataframe(df_route, use_container_width=True, hide_index=True)

with geo_col2:
    st.subheader("Top Airports by Delay")
    df_airport = load_data(f"SELECT airport_code, ROUND(AVG(delay_rate) * 100, 2) AS rate FROM flight_delay.agg_airport_performance WHERE year BETWEEN {selected_year[0]} AND {selected_year[1]} GROUP BY airport_code ORDER BY rate DESC LIMIT 10")
    st.dataframe(df_airport, use_container_width=True, hide_index=True)