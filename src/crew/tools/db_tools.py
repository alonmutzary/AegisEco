import os
import psycopg2
from crewai.tools import tool
from dotenv import load_dotenv

load_dotenv()

@tool("Query High Rainfall Events")
def get_high_rainfall_events(threshold_mm: float) -> str:
    """
    Queries the database for any rain measurements that exceeded the given threshold in mm.
    Returns a formatted string of the results to be analyzed by the agent.
    """
    # Use the DB URL from your .env file
    db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        return "Error: DATABASE_URL is missing from environment variables."

    query = """
        SELECT station_name, measurement_time, rain_amount_mm
        FROM rain_measurements
        WHERE rain_amount_mm >= %s
        ORDER BY rain_amount_mm DESC;
    """
    
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # Execute the query safely with the parameter
        cur.execute(query, (threshold_mm,))
        rows = cur.fetchall()

        if not rows:
            return f"No stations recorded rainfall above {threshold_mm}mm."

        result = f"Found {len(rows)} records exceeding {threshold_mm}mm:\n"
        for row in rows:
            station_name, m_time, rain = row
            # Format the output so the LLM can easily read it
            result += f"- Station '{station_name}' at {m_time}: {rain}mm\n"

        return result
        
    except Exception as e:
        return f"Database query failed: {e}"
        
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()