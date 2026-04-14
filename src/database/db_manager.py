import os
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime, timedelta
from src.data_sentinel.ims_client import get_all_latest_rain_records, get_february_data_all_stations
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def save_ims_batch_to_db(records: list):
    """
    Saves a batch of rain records to the database.
    Ignores records for stations that do not exist in the 'stations' table
    to prevent Foreign Key violation errors.
    """
    if not records:
        print("No records to save.")
        return

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("Error: DATABASE_URL not found in environment.")
        return

    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()

        # Prepare the data tuples for bulk insertion
        values_to_insert = [
            (
                rec['station_id'],
                rec['station_name'],
                rec['measurement_time'],
                rec['rain_amount_mm'],
                rec.get('region_id', None),
                rec.get('status', 1)
            )
            for rec in records
        ]
        # Check which stations are missing from the local database
        cursor.execute("SELECT station_id FROM stations;")
        existing_station_ids = {row[0] for row in cursor.fetchall()}
        
        missing_stations = []
        for rec in records:
            if rec['station_id'] not in existing_station_ids:
                missing_stations.append(f"{rec['station_id']} ({rec['station_name']})")
                
        if missing_stations:
            print(f"Skipping {len(missing_stations)} unregistered stations: {', '.join(missing_stations)}")
            
        # The safe SQL query: 
        # Casts measurement_time to timestamp and filters out unknown stations.
        insert_query = """
            INSERT INTO rain_measurements (
                station_id, station_name, measurement_time, rain_amount_mm, region_id, status
            )
            SELECT v.station_id, v.station_name, v.measurement_time::timestamp, v.rain_amount_mm, v.region_id, v.status
            FROM (VALUES %s) AS v(station_id, station_name, measurement_time, rain_amount_mm, region_id, status)
            WHERE EXISTS (
                SELECT 1 FROM stations s WHERE s.station_id = v.station_id
            )
            ON CONFLICT DO NOTHING;
        """

        # Using execute_values for efficient bulk insert
        execute_values(cursor, insert_query, values_to_insert)
        conn.commit()
        
        # Determine how many rows were actually inserted (optional, but good for logging)
        inserted_count = cursor.rowcount
        print(f"Successfully saved {inserted_count} records to 'rain_measurements'.")

    except Exception as e:
        print(f"Database Error during save: {e}")
        if 'conn' in locals() and conn:
            conn.rollback() # Rollback on error
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def save_ims_data_to_db(record):
    if not record:
        print("No record provided to save.")
        return

    try:
        # Connect to the shared cloud database
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        cur = conn.cursor()
        
        # SQL Query using named placeholders to match the dictionary keys
        insert_query = """
        INSERT INTO rain_measurements (
            station_id, 
            rain_amount_mm, 
            measurement_time, 
            station_name, 
            region_id, 
            status
        )
        VALUES (
            %(station_id)s, 
            %(rain_amount_mm)s, 
            %(measurement_time)s, 
            %(station_name)s, 
            %(region_id)s, 
            %(status)s
        );
        """
        
        # Execute using the record dictionary directly
        cur.execute(insert_query, record)
        
        print(f"Successfully saved {record['rain_amount_mm']}mm for {record['station_name']} (ID: {record['station_id']})")
        
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Database error: {e}")

def fetch_latest_all_stations():
    records = get_all_latest_rain_records()
    for record in records:
        save_ims_data_to_db(record)

def fetch_feb_records():
    feb_records = get_february_data_all_stations()
    save_ims_batch_to_db(feb_records)

if __name__ == "__main__":
    # fetch_latest_all_stations()
    fetch_feb_records()