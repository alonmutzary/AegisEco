import os
import re
import sys

import psycopg2
import streamlit as st


def _project_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


@st.cache_data(ttl=3600)
def fetch_basin_geometries(db_url: str) -> dict:
    """Returns {main_basin_name: geojson_string} with WGS84 coordinates."""
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("""
            SELECT main_basin_name,
                   ST_AsGeoJSON(ST_Transform(ST_Union(geometry), 4326)) AS geojson
            FROM basins
            WHERE main_basin_name IS NOT NULL
            GROUP BY main_basin_name
            ORDER BY main_basin_name
        """)
        return {row[0]: row[1] for row in cur.fetchall() if row[1]}
    except Exception:
        return {}
    finally:
        if "cur" in locals(): cur.close()
        if "conn" in locals(): conn.close()


@st.cache_data(ttl=3600)
def fetch_basin_roads(db_url: str) -> dict:
    """Returns {main_basin_name: [road, ...]}."""
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT main_basin_name, unnest(main_roads) AS road
            FROM basins
            WHERE main_basin_name IS NOT NULL
              AND main_roads IS NOT NULL
              AND array_length(main_roads, 1) > 0
            ORDER BY main_basin_name, road
        """)
        result = {}
        for name, road in cur.fetchall():
            result.setdefault(name, []).append(road)
        return result
    except Exception:
        return {}
    finally:
        if "cur" in locals(): cur.close()
        if "conn" in locals(): conn.close()


@st.cache_data(ttl=300)
def get_flood_status(db_url: str) -> dict:
    """
    Runs ML inference for all basins via the src package.
    Returns {basin_name: {"probability": float, "alert": bool, "horizon": str}}.
    Falls back to empty dict if the src package is unreachable.
    """
    root = _project_root()
    if root not in sys.path:
        sys.path.insert(0, root)

    # Ensure DATABASE_URL is available for db_manager imports
    os.environ.setdefault("DATABASE_URL", db_url)

    try:
        from src.crew.tools.db_tools import _run_all_basins_inference
        return _parse_inference_report(_run_all_basins_inference())
    except Exception:
        return {}


def _parse_inference_report(report: str) -> dict:
    status = {}
    for line in report.splitlines():
        prob_match = re.search(r"Probability:\s*([\d.]+)%", line)
        if not prob_match:
            continue
        prob = float(prob_match.group(1)) / 100.0
        alert = "CRITICAL ALERT" in line
        horizon_match = re.search(r"\((\d+h)\s+forecast\)", line)
        horizon = horizon_match.group(1) if horizon_match else "?"

        if alert:
            name_match = re.search(r"CRITICAL ALERT - ([^:]+):", line)
        else:
            name_match = re.search(r"✅\s+(.+?)\s+\(\d+h", line)

        if name_match:
            status[name_match.group(1).strip()] = {
                "probability": prob,
                "alert": alert,
                "horizon": horizon,
            }
    return status
