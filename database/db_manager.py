import os
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime, timedelta
from data_sentinel.ims_client import get_all_latest_rain_records, get_february_data_all_stations
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def save_ims_batch_to_db(records_list, batch_size=2000):
    """
    Takes the full list of February dictionaries and pushes them 
    to Neon in optimized chunks.
    """
    if not records_list:
        print("No records provided to save.")
        return

    # Use your existing DATABASE_URL
    DATABASE_URL = "postgresql://neondb_owner:npg_gtWTp2KxDe7Y@ep-broad-fog-aimsm40u-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require"

    # We use a standard VALUES %s syntax for execute_values
    # ON CONFLICT ensures we don't get errors if a record already exists
    insert_query = """
    INSERT INTO rain_measurements (
        station_id, 
        rain_amount_mm, 
        measurement_time, 
        station_name, 
        region_id, 
        status
    )
    VALUES %s
    ON CONFLICT (station_id, measurement_time) DO NOTHING;
    """

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Convert the list of dictionaries into a list of tuples for the batch
        # This matches the column order in the INSERT statement above
        data_tuples = [
            (
                r['station_id'], 
                r['rain_amount_mm'], 
                r['measurement_time'], 
                r['station_name'], 
                r['region_id'], 
                r['status']
            ) for r in records_list
        ]

        print(f"--- Starting Bulk Upload to Neon ({len(data_tuples)} records) ---")
        
        # This is the magic part: it pushes batch_size records in a single network trip
        execute_values(cur, insert_query, data_tuples, page_size=batch_size)
        
        conn.commit() # Save all changes at once
        print(f"✅ Successfully processed {len(data_tuples)} records in the database.")

    except Exception as e:
        print(f"Database batch error: {e}")
        if conn: conn.rollback()
    finally:
        if cur: cur.close()
        if conn: conn.close()

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

# fetch_latest_all_stations()
fetch_feb_records()