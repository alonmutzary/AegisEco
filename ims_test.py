import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
load_dotenv()

# Configuration
API_TOKEN = os.getenv("IMS_API_KEY")
STATION_ID = 21        
BASE_URL = "https://api.ims.gov.il/v1/envista/stations"

headers = {
    'Authorization': f'ApiToken {API_TOKEN}' # Must follow this exact format 
}

def get_rain_data_by_station(station_id):
    # 1. Fetch metadata to find the specific Channel ID for 'Rain' at this station
    # Station channels can vary between different locations 
    meta_url = f"{BASE_URL}/{station_id}"
    meta_response = requests.get(meta_url, headers=headers)
    
    if meta_response.status_code != 200:
        print(f"Error fetching metadata: {meta_response.status_code}")
        return

    station_info = meta_response.json()
    rain_channel_id = None

    # Search for the Rain channel in the monitors list [cite: 90, 94]
    for monitor in station_info.get('monitors', []):
        if monitor.get('name') == 'Rain':
            rain_channel_id = monitor.get('channelId')
            break

    if not rain_channel_id:
        print(f"Rain channel not found for station {station_id}")
        return

    # 2. Fetch the latest measurement for the identified Rain channel [cite: 152]
    data_url = f"{BASE_URL}/{station_id}/data/{rain_channel_id}/daily/2026/03/04"
    data_response = requests.get(data_url, headers=headers)

    if data_response.status_code == 200 and data_response.text.strip():
        data_json = data_response.json()
        measurements = data_json.get('data', [])
        
        # 3. Sum the values of all valid 10-minute intervals
        total_rain = 0.0
        for entry in measurements:
            channel = entry['channels'][0]
            # status 1 = Valid, status 2 = Invalid 
            if channel.get('status') == 1:
                total_rain += channel.get('value', 0.0)
        
        return total_rain
    else:
        print(f"Failed to retrieve daily data. Status: {data_response.status_code}")
        return None
    
def get_all_rain_data():
    response = requests.get(BASE_URL, headers=headers)
    if response.status_code != 200:
        return

    stations = response.json()

    for station in stations:
        station_id = station.get('stationId')
        station_name = station.get('name')
        
        # Find the Rain channel [cite: 14, 94]
        rain_channel_id = next((m.get('channelId') for m in station.get('monitors', []) 
                                if m.get('name') == 'Rain'), None)

        if not rain_channel_id:
            continue

        data_url = f"{BASE_URL}/{station_id}/data/{rain_channel_id}/monthly/2026/02"
        data_resp = requests.get(data_url, headers=headers)

        # FIX: Check if the response is successful AND has content
        if data_resp.status_code == 200 and data_resp.text.strip():
            try:
                data_json = data_resp.json()
                if data_json.get('data'):
                    measure = data_json['data'][0]
                    channel = measure['channels'][0]
                    
                    val = channel['value'] # [cite: 112, 135]
                    status = channel['status'] # 
                    time = measure['datetime'] # [cite: 125]
                    
                    if status == 1:
                        print(f"Station: {station_name} ({station_id}) | Rain: {val}mm | Time: {time} LST")
                    else:
                        print(f"Station: {station_name} - Invalid Data Reported")
            except requests.exceptions.JSONDecodeError:
                print(f"Station: {station_name} - Received empty or invalid JSON")
        else:
            print(f"Station: {station_name} - Request failed with status: {data_resp.status_code}")

def analyze_rain_trend(station_id):
    # 1. Get metadata to find the Rain channel ID [cite: 65, 94]
    meta_url = f"{BASE_URL}/{station_id}"
    meta_resp = requests.get(meta_url, headers=headers)
    if meta_resp.status_code != 200: return

    rain_channel_id = next((m.get('channelId') for m in meta_resp.json().get('monitors', []) 
                            if m.get('name') == 'Rain'), None)

    if not rain_channel_id: return

    # 2. Fetch daily data (10-minute intervals) [cite: 13, 157]
    # This provides a list of all measurements taken today
    daily_url = f"{BASE_URL}/{station_id}/data/{rain_channel_id}/daily"
    data_resp = requests.get(daily_url, headers=headers)

    if data_resp.status_code == 200 and data_resp.text.strip():
        measurements = data_resp.json().get('data', [])
        
        if len(measurements) < 2:
            print("Not enough data points yet to calculate a trend.")
            return

        # Sort by time (most recent first) [cite: 125]
        measurements.reverse() 

        latest = measurements[0]['channels'][0]['value']
        one_hour_ago = measurements[6]['channels'][0]['value'] if len(measurements) > 6 else measurements[-1]['channels'][0]['value']

        # Calculate Intensity (mm per hour)
        intensity = latest - one_hour_ago
        
        print(f"--- Trend Analysis: Station {station_id} ---")
        print(f"Current Rain: {latest} mm [cite: 216]")
        print(f"Rain 1 hour ago: {one_hour_ago} mm")
        print(f"Intensity: {intensity:.2f} mm/hr")

        # Flash Flood Logic for AegisEco Agents
        if intensity > 5.0:
            print("ALERT: High intensity detected! Potential Flash Flood Risk.")
        elif intensity > 0:
            print("Status: Moderate rainfall increasing.")
        else:
            print("Status: Stable/No rain.")

if __name__ == "__main__":
    print(get_rain_data_by_station(STATION_ID))
    # get_all_rain_data()
    # analyze_rain_trend(STATION_ID)