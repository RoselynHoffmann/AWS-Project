# app.py: displays dashboard in hopefully a pretty way

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Equipment Telemetry Dashboard", page_icon="⚙️", layout="wide")

@st.cache_resource
def get_connection():
    return duckdb.connect("warehouse/telemetry.duckdb", read_only=True)

conn = get_connection()
def query(sql): return conn.execute(sql).fetchdf()

st.markdown("""
<style>
    .metric-card { background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%); padding: 1.2rem; border-radius: 12px; border: 1px solid #334155; text-align: center; }
    .metric-card h3 { color: #94a3b8; font-size: 0.85rem; margin-bottom: 0.3rem; font-weight: 500; }
    .metric-card .value { font-size: 1.8rem; font-weight: 700; }
</style>
""", unsafe_allow_html=True)

st.markdown("# ⚙️ Equipment Telemetry Dashboard")
st.markdown("Real-time monitoring for industrial ore analyzers across global sites.")
st.divider()

health_df = query("SELECT * FROM mart_equipment_health")
hourly_df = query("SELECT * FROM mart_hourly_dashboard ORDER BY hour_timestamp")
anomaly_df = query("SELECT * FROM mart_anomaly_events ORDER BY reading_timestamp DESC")

st.sidebar.header("Filters")
all_machines = sorted(health_df["machine_id"].unique())
selected_machines = st.sidebar.multiselect("Machines", all_machines, default=all_machines)
all_sites = sorted(health_df["site"].unique())
selected_sites = st.sidebar.multiselect("Sites", all_sites, default=all_sites)

st.sidebar.header("Alert Thresholds")
temp_threshold = st.sidebar.slider("Temperature Warning (°C)", 50, 100, 80)
vibration_threshold = st.sidebar.slider("Vibration Warning (mm/s)", 1.0, 10.0, 5.0, 0.5)

filtered_health = health_df[(health_df["machine_id"].isin(selected_machines)) & (health_df["site"].isin(selected_sites))]
filtered_hourly = hourly_df[(hourly_df["machine_id"].isin(selected_machines)) & (hourly_df["site"].isin(selected_sites))]

st.subheader("Fleet Overview")
c1, c2, c3, c4, c5 = st.columns(5)
with c1: st.metric("Total Machines", len(filtered_health))
with c2: st.metric("Healthy", len(filtered_health[filtered_health["health_status"]=="healthy"]))
with c3: st.metric("Warning", len(filtered_health[filtered_health["health_status"]=="warning"]))
with c4: st.metric("Critical", len(filtered_health[filtered_health["health_status"]=="critical"]))
with c5: st.metric("Anomaly Events", len(anomaly_df[anomaly_df["machine_id"].isin(selected_machines)]))

st.subheader("Machine Status")
if len(filtered_health) > 0:
    cols = st.columns(min(len(filtered_health), 5))
    for idx, (_, m) in enumerate(filtered_health.iterrows()):
        with cols[idx % len(cols)]:
            s = m["health_status"]
            emoji = {"healthy":"🟢","warning":"🟡","critical":"🔴"}.get(s,"⚪")
            color = {"healthy":"#4ade80","warning":"#fbbf24","critical":"#f87171"}.get(s,"#94a3b8")
            st.markdown(f'<div class="metric-card"><h3>{m["machine_id"]}</h3><div class="value" style="color:{color};">{emoji} {s.upper()}</div><p style="color:#94a3b8;font-size:0.8rem;margin:0.5rem 0 0 0;">{m["site"]}<br>Temp: {m["current_temp"]:.1f}°C | Vib: {m["current_vibration"]:.2f} mm/s</p></div>', unsafe_allow_html=True)
            st.write("")

st.subheader("Temperature Trends")
fig = px.line(filtered_hourly, x="hour_timestamp", y="avg_temp_c", color="machine_id", template="plotly_dark", labels={"hour_timestamp":"Time","avg_temp_c":"Avg Temp (°C)","machine_id":"Machine"})
fig.add_hline(y=temp_threshold, line_dash="dash", line_color="red", annotation_text=f"Warning ({temp_threshold}°C)", annotation_position="top left")
fig.update_layout(height=400, margin=dict(l=0,r=0,t=30,b=0), legend=dict(orientation="h",yanchor="bottom",y=1.02))
st.plotly_chart(fig, use_container_width=True)

cl, cr = st.columns(2)
with cl:
    st.subheader("Vibration Trends")
    fv = px.line(filtered_hourly, x="hour_timestamp", y="avg_vibration", color="machine_id", template="plotly_dark", labels={"hour_timestamp":"Time","avg_vibration":"Vibration (mm/s)"})
    fv.add_hline(y=vibration_threshold, line_dash="dash", line_color="orange", annotation_text=f"Threshold ({vibration_threshold})")
    fv.update_layout(height=350, margin=dict(l=0,r=0,t=30,b=0), showlegend=False)
    st.plotly_chart(fv, use_container_width=True)
with cr:
    st.subheader("Power Consumption")
    fp = px.line(filtered_hourly, x="hour_timestamp", y="avg_power_kw", color="machine_id", template="plotly_dark", labels={"hour_timestamp":"Time","avg_power_kw":"Power (kW)"})
    fp.update_layout(height=350, margin=dict(l=0,r=0,t=30,b=0), showlegend=False)
    st.plotly_chart(fp, use_container_width=True)

cl2, cr2 = st.columns(2)
with cl2:
    st.subheader("Avg Throughput (samples/hr)")
    ft = px.bar(filtered_hourly.groupby("machine_id")["avg_throughput"].mean().reset_index(), x="machine_id", y="avg_throughput", color="machine_id", template="plotly_dark")
    ft.update_layout(height=350, margin=dict(l=0,r=0,t=30,b=0), showlegend=False)
    st.plotly_chart(ft, use_container_width=True)
with cr2:
    st.subheader("Availability (%)")
    fa = px.bar(filtered_hourly.groupby("machine_id")["running_pct"].mean().reset_index(), x="machine_id", y="running_pct", color="machine_id", template="plotly_dark")
    fa.update_layout(height=350, margin=dict(l=0,r=0,t=30,b=0), showlegend=False)
    st.plotly_chart(fa, use_container_width=True)

st.subheader("Anomaly Events")
if len(anomaly_df) > 0:
    disp = anomaly_df[anomaly_df["machine_id"].isin(selected_machines)][["reading_timestamp","machine_id","site","alert_level","temperature_c","vibration_mm_s","error_code","error_description"]].head(50)
    st.dataframe(disp, column_config={"reading_timestamp":st.column_config.DatetimeColumn("Time",format="YYYY-MM-DD HH:mm"),"machine_id":"Machine","site":"Site","alert_level":"Severity","temperature_c":st.column_config.NumberColumn("Temp (°C)",format="%.1f"),"vibration_mm_s":st.column_config.NumberColumn("Vibration",format="%.2f"),"error_code":"Error Code","error_description":"Description"}, use_container_width=True, hide_index=True)

st.divider()
st.subheader("Machine Deep Dive")
sel = st.selectbox("Select a machine", all_machines)
if sel:
    md = hourly_df[hourly_df["machine_id"]==sel]
    mh = health_df[health_df["machine_id"]==sel].iloc[0]
    ca,cb,cc,cd = st.columns(4)
    with ca: st.metric("Current Temp", f"{mh['current_temp']:.1f}°C")
    with cb: st.metric("Current Vibration", f"{mh['current_vibration']:.2f} mm/s")
    with cc: st.metric("Current Power", f"{mh['current_power']:.1f} kW")
    with cd: st.metric("24h Errors", int(mh.get("total_errors_24h",0)))
    fd = go.Figure()
    for metric,label,color in [("avg_temp_c","Temperature","#f87171"),("avg_vibration","Vibration","#fbbf24"),("avg_power_kw","Power","#60a5fa")]:
        vals = md[metric]; norm = (vals-vals.min())/(vals.max()-vals.min()+0.001)
        fd.add_trace(go.Scatter(x=md["hour_timestamp"],y=norm,name=label,line=dict(color=color)))
    fd.update_layout(title=f"Normalized Metrics — {sel}", template="plotly_dark", height=400, margin=dict(l=0,r=0,t=40,b=0), yaxis_title="Normalized (0-1)", legend=dict(orientation="h",yanchor="bottom",y=1.02))
    st.plotly_chart(fd, use_container_width=True)

st.divider()
st.caption("Industrial Equipment Telemetry Pipeline: Python, AWS S3, DuckDB, dbt, Streamlit")
