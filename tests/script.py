import os
import math
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from thefuzz import process, fuzz

# --- CONFIGURATION ---
XML_URL = "https://ims.gov.il/sites/default/files/ims_data/xml_files/isr_cities_1week_6hr_forecast.xml"
OUTPUT_FILE = "ims_to_db_mapping_review.csv"
MAX_DISTANCE_KM = 5.0 # <--- The spatial filter threshold

def calculate_distance(lat1, lon1, lat2, lon2):
    """
    Calculates the distance in kilometers between two points on Earth using the Haversine formula.
    """
    if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
        return float('inf') # Return infinity if coordinates are missing

    R = 6371.0 # Earth radius in kilometers

    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance

def get_ims_cities():
    """Fetches the XML and extracts LocationId, Name, Lat, and Lon."""
    print("📡 Fetching cities and coordinates from IMS XML...")
    response = requests.get(XML_URL)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    
    ims_cities = []
    for loc in root.findall('.//LocationMetaData'):
        loc_id = loc.find('LocationId').text
        name_eng = loc.find('LocationNameEng').text
        lat_str = loc.find('DisplayLat').text
        lon_str = loc.find('DisplayLon').text
        
        # Convert coordinates to floats, handle potential missing/bad data safely
        try:
            lat = float(lat_str) if lat_str else None
            lon = float(lon_str) if lon_str else None
        except ValueError:
            lat = None
            lon = None

        ims_cities.append({
            'ims_id': loc_id, 
            'ims_name': name_eng,
            'lat': lat,
            'lon': lon
        })
    
    print(f"✅ Found {len(ims_cities)} cities in XML.")
    return ims_cities

def get_db_settlements():
    """Connects to Neon DB and fetches settlement_id, names, lat, and lon."""
    print("🐘 Connecting to Neon Database...")
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL not found in .env file.")
        
    conn = psycopg2.connect(db_url)
    # Added lat and lon to the SQL query
    query = "SELECT settlement_id, name_eng, name_heb, lat, lon FROM settlements;"
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"✅ Found {len(df)} settlements in Database.")
    return df

def find_best_matches():
    """Matches IMS cities to DB settlements using spatial filtering + fuzzy logic."""
    load_dotenv()
    
    ims_cities = get_ims_cities()
    db_df = get_db_settlements()
    
    results = []
    
    print(f"🔍 Starting matching process (Spatial Filter: {MAX_DISTANCE_KM}km max)...")
    
    for city in ims_cities:
        ims_name = city['ims_name']
        ims_lat = city['lat']
        ims_lon = city['lon']
        
        best_db_name = "NO MATCH FOUND"
        best_db_id = None
        highest_score = -1
        best_distance = None

        # Iterate through EVERY settlement in the database to find the best match
        for index, row in db_df.iterrows():
            db_name = str(row['name_eng']) if pd.notna(row['name_eng']) else ""
            db_lat = row['lat']
            db_lon = row['lon']
            
            # 1. SPATIAL FILTER: Check distance first
            distance_km = calculate_distance(ims_lat, ims_lon, db_lat, db_lon)
            
            if distance_km > MAX_DISTANCE_KM:
                continue # Skip this settlement completely if it's too far!
                
            # 2. STRING MATCHING: Only calculate score if it passed the distance test
            # Using simple fuzz.ratio as it handles Hebrew/English names better
            score = fuzz.ratio(ims_name.lower(), db_name.lower())
            
            # Keep track of the highest scoring match that is WITHIN the radius
            if score > highest_score:
                highest_score = score
                best_db_name = db_name
                best_db_id = row['settlement_id']
                best_distance = round(distance_km, 2)

        results.append({
            'IMS_ID': city['ims_id'],
            'IMS_Name': ims_name,
            'Matched_DB_ID': best_db_id,
            'Matched_DB_Name': best_db_name,
            'Distance_KM': best_distance,
            'Confidence_Score': highest_score if highest_score != -1 else 0
        })
        
    results_df = pd.DataFrame(results)
    
    # Sort by score ascending, so NO MATCH FOUND (score 0) is at the top
    results_df = results_df.sort_values(by='Confidence_Score')
    
    results_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"\n🎉 Matching complete! Results saved to '{OUTPUT_FILE}'")
    print("⚠️  PLEASE REVIEW THE CSV FILE. Pay attention to 'NO MATCH FOUND' rows.")

if __name__ == "__main__":
    find_best_matches()