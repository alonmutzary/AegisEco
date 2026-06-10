import json
import os

import folium
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

from utils.db_connector import run_query
from utils.map_helpers import fetch_basin_geometries, fetch_basin_roads, get_flood_status
from utils.permissions import has_access

current_dir = os.path.dirname(os.path.abspath(__file__))
logo_path    = os.path.join(current_dir, "assets", "logo.jpg")
favicon_path = os.path.join(current_dir, "assets", "favicon.jpg")

st.set_page_config(
    page_title="AegisEco Dashboard",
    page_icon=favicon_path,
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
    <style>
    .main-status-box {
        padding: 25px;
        border-radius: 12px;
        color: white;
        text-align: center;
        font-size: 28px;
        font-weight: bold;
        margin-bottom: 20px;
        box-shadow: 0 4px 10px rgba(0,0,0,0.15);
    }
    .status-ok     { background: linear-gradient(90deg, #28a745, #218838); }
    .status-danger {
        background: linear-gradient(90deg, #dc3545, #c82333);
        animation: pulse-red 2s infinite;
    }
    @keyframes pulse-red {
        0%   { box-shadow: 0 0 0 0   rgba(220, 53, 69, 0.7); }
        70%  { box-shadow: 0 0 0 15px rgba(220, 53, 69, 0);   }
        100% { box-shadow: 0 0 0 0   rgba(220, 53, 69, 0);   }
    }
    </style>
""", unsafe_allow_html=True)

# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image(logo_path, use_container_width=True)
    st.divider()
    st.header("System Settings")
    user_role = st.selectbox(
        "Select User Role:",
        ["Citizen", "City", "Authority"],
        help="Change access level to view different system modules",
    )
    st.sidebar.divider()
    st.sidebar.info(f"Connected as: **{user_role}**")

# ── Live flood status (drives the banner and the map) ────────────────────────
db_url       = st.secrets["DATABASE_URL"]
flood_status = get_flood_status(db_url)
is_flood_danger = any(v.get("alert", False) for v in flood_status.values())
alert_basins    = [k for k, v in flood_status.items() if v.get("alert")]

# ── Global status banner ─────────────────────────────────────────────────────
if is_flood_danger:
    basin_list = ", ".join(alert_basins)
    st.markdown(
        f'<div class="main-status-box status-danger">'
        f'⚠️ FLOOD DANGER DETECTED — {basin_list}'
        f'</div>',
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        '<div class="main-status-box status-ok">'
        "✅ ALL SYSTEMS NORMAL — NO IMMEDIATE THREATS"
        "</div>",
        unsafe_allow_html=True,
    )

# ── Tabs ─────────────────────────────────────────────────────────────────────
available_tabs = []
if has_access(user_role, "view_risk_map"):        available_tabs.append("Risk Map")
if has_access(user_role, "view_city_dashboard"):  available_tabs.append("City Control Center")
if has_access(user_role, "view_basins_data"):     available_tabs.append("Councils Info")
if has_access(user_role, "view_system_logs"):     available_tabs.append("System Logs")
if has_access(user_role, "view_social_feed"):     available_tabs.append("Social Updates")

tabs      = st.tabs(available_tabs)
tab_index = 0

# ── TAB: RISK MAP ─────────────────────────────────────────────────────────────
if has_access(user_role, "view_risk_map"):
    with tabs[tab_index]:
        st.subheader("Regional Risk Assessment Map")

        col_refresh, col_status = st.columns([1, 6])
        with col_refresh:
            if st.button("🔄 Refresh"):
                st.cache_data.clear()
                st.rerun()
        with col_status:
            if not flood_status:
                st.warning("ML inference unavailable — map shown without flood status.")
            elif alert_basins:
                st.error(f"🚨 Active alerts: **{', '.join(alert_basins)}**")
            else:
                st.success("✅ No active flood alerts")

        # Fetch geometry + roads
        basins_geo  = fetch_basin_geometries(db_url)
        basin_roads = fetch_basin_roads(db_url)

        # Build folium map
        m = folium.Map(location=[31.5, 34.9], zoom_start=7, tiles="CartoDB positron")

        if basins_geo:
            for basin_name, geojson_str in basins_geo.items():
                status   = flood_status.get(basin_name, {})
                is_alert = status.get("alert", False)
                prob     = status.get("probability", 0.0)
                horizon  = status.get("horizon", "?")

                if not flood_status:
                    fill_color, opacity = "#6c757d", 0.3   # grey — status unknown
                elif is_alert:
                    fill_color, opacity = "#dc3545", 0.65  # red
                else:
                    fill_color, opacity = "#28a745", 0.30  # green

                roads     = basin_roads.get(basin_name, [])
                roads_html = "".join(f"<br>• {r}" for r in roads) or "<br><i>No road data</i>"

                popup_html = f"""
                <div style="font-family:Arial; min-width:230px; padding:4px">
                  <b style="font-size:14px">{basin_name} Basin</b><br><br>
                  {"<b style='color:#dc3545'>🚨 FLOOD ALERT</b>" if is_alert
                   else "<b style='color:#28a745'>✅ Status Normal</b>"}<br>
                  Probability: <b>{prob * 100:.1f}%</b> ({horizon} horizon)<br>
                  <br><b>Affected Roads:</b>{roads_html}
                </div>
                """

                try:
                    folium.GeoJson(
                        json.loads(geojson_str),
                        style_function=lambda _, fc=fill_color, op=opacity: {
                            "fillColor": fc,
                            "color": "#444444",
                            "weight": 1.5,
                            "fillOpacity": op,
                        },
                        tooltip=folium.Tooltip(f"<b>{basin_name}</b> — {prob * 100:.1f}%"),
                        popup=folium.Popup(popup_html, max_width=320),
                    ).add_to(m)
                except Exception:
                    pass
        else:
            st.warning("Basin geometry could not be loaded from the database. Showing base map.")

        # Legend
        legend_html = """
        <div style="position:fixed; bottom:30px; left:30px; z-index:1000;
                    background:white; padding:12px 16px; border-radius:8px;
                    box-shadow:0 2px 8px rgba(0,0,0,0.2); font-family:Arial; font-size:13px">
          <b>Basin Status</b><br>
          <span style="background:#dc3545;display:inline-block;width:14px;height:14px;
                       border-radius:3px;margin-right:6px;vertical-align:middle"></span>Flood Alert<br>
          <span style="background:#28a745;display:inline-block;width:14px;height:14px;
                       border-radius:3px;margin-right:6px;vertical-align:middle"></span>Normal<br>
          <span style="background:#6c757d;display:inline-block;width:14px;height:14px;
                       border-radius:3px;margin-right:6px;vertical-align:middle"></span>Status Unknown
        </div>
        """
        m.get_root().html.add_child(folium.Element(legend_html))

        st_folium(m, use_container_width=True, height=560, returned_objects=[])

        # Probability table below the map
        if flood_status:
            st.subheader("Basin Probabilities")
            table_rows = [
                {
                    "Basin": name,
                    "Status": "🚨 ALERT" if s["alert"] else "✅ Normal",
                    "Flood Probability": f"{s['probability'] * 100:.1f}%",
                    "Forecast Horizon": s["horizon"],
                }
                for name, s in sorted(flood_status.items())
            ]
            st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)

    tab_index += 1

# ── TAB: CITY CONTROL CENTER ──────────────────────────────────────────────────
if has_access(user_role, "view_city_dashboard"):
    with tabs[tab_index]:
        st.subheader("🏙️ City Level Dashboard")
        st.write("Search and select your municipality to view localized data and alerts.")

        try:
            settlements_df = run_query("SELECT DISTINCT name_eng FROM settlements ORDER BY name_eng;")
            city_list = settlements_df["name_eng"].tolist() if not settlements_df.empty else []
        except Exception as e:
            st.error(f"Failed to fetch settlements from DB: {e}")
            city_list = []

        if city_list:
            selected_city = st.selectbox("Type to search for your city:", city_list)
            st.divider()

            if selected_city:
                safe_city_name = selected_city.replace("'", "''")
                try:
                    city_df = run_query(f"""
                        SELECT name_eng, current_6h_forecast, next_6h_forecast
                        FROM settlements
                        WHERE name_eng = '{safe_city_name}'
                        LIMIT 1;
                    """)

                    if not city_df.empty:
                        city_row = city_df.iloc[0]
                        st.markdown(f"### Status for **{city_row['name_eng']}**")

                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Current 6H Forecast (mm)", city_row["current_6h_forecast"])
                        with col2:
                            st.metric("Next 6H Forecast (mm)", city_row["next_6h_forecast"])
                        with col3:
                            st.metric("Rainfall Last Hour (mm)", "to be implemented")

                        has_local_alert = (
                            city_row["current_6h_forecast"] > 0
                            or city_row["next_6h_forecast"] > 0
                        )

                        if has_local_alert:
                            st.error(f"🚨 ACTIVE FLOOD WARNING FOR {city_row['name_eng'].upper()}")
                            st.warning("""
                            **Required Municipality Actions:**
                            1. Deploy municipal pumps to known low-elevation areas.
                            2. Clear drainage grates and sewage entries immediately.
                            3. Prepare emergency response teams for deployment.
                            """)
                        else:
                            st.success(f"✅ No active warnings for {city_row['name_eng']}. Conditions are optimal.")

                except Exception as e:
                    st.error(f"Error loading data for {selected_city}: {e}")
        else:
            st.warning("No cities found in the database. Please check the 'settlements' table.")

    tab_index += 1

# ── TAB: COUNCILS INFO ────────────────────────────────────────────────────────
if has_access(user_role, "view_basins_data"):
    with tabs[tab_index]:
        st.subheader("Regional Councils Registry")
        st.write("Live data from the 'councils' database table:")
        try:
            councils_df = run_query("SELECT * FROM councils;")
            st.dataframe(councils_df, use_container_width=True)
        except Exception as e:
            st.error(f"Error fetching data: {e}")
    tab_index += 1

# ── TAB: SYSTEM LOGS ─────────────────────────────────────────────────────────
if has_access(user_role, "view_system_logs"):
    with tabs[tab_index]:
        st.subheader("Live Agent Activity")
        st.write("Monitoring internal communications between AI agents:")
        st.code("real time logs will be displayed here", language="bash")
    tab_index += 1

# ── TAB: SOCIAL UPDATES ───────────────────────────────────────────────────────
if has_access(user_role, "view_social_feed"):
    with tabs[tab_index]:
        st.subheader("Social Media Monitoring")
        st.write("Aggregated reports from Twitter and Facebook:")
        st.warning("Social listening agents are currently inactive.")
    tab_index += 1
