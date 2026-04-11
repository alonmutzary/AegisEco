import pandas as pd
import matplotlib.pyplot as plt
import os

# ==========================================
# 1. Settings and Basin Name Extraction
# ==========================================
FILE_PATH = r'..\flow_data\sorek_2010_2019.csv'

filename = os.path.basename(FILE_PATH)            
basin_name = filename.split('_')[0].capitalize()   
print(f"Loading raw data for {basin_name} basin from {filename}...")

# 2. Read the file
df = pd.read_csv(FILE_PATH, header=None, encoding='utf-8')

# 3. Rename columns
df.columns = [
    'Record_ID', 'Station_Code', 'Name_Hebrew', 'Name_English', 'Hydro_Year',
    'DateTime', 'Water_Level', 'Flow', 'Measurement_Type', 'Water_Type', 'Segment_Status'
]

# 4. Convert date to datetime format 
df['DateTime'] = pd.to_datetime(df['DateTime'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
df = df.dropna(subset=['DateTime'])

# 5. Set DateTime as index
df = df.set_index('DateTime')
df_flow = df[['Flow']].copy()

print(f"Raw data rows: {len(df_flow):,}")

# 6. Resampling - Align to an hourly grid
print("Aligning data to a fixed 1-hour grid...")
df_hourly = df_flow.resample('1h').max()
df_hourly = df_hourly.ffill().fillna(0)

# ==========================================
# 7. Check results and dynamic printing
# ==========================================
flood_hours = (df_hourly['Flow'] > 5.0).sum()

print(f"\n=== AegisEco: {basin_name} Basin Preprocessing ===")
print(f"Total hourly rows (Grid): {len(df_hourly):,}")
print(f"Hours with FLOOD (>5.0 m3/s): {flood_hours:,}")
print(f"Max flow recorded: {df_hourly['Flow'].max()} m3/s")

# Visualize the extreme event - dynamic title
max_flow_time = df_hourly['Flow'].idxmax()
print(f"\nPlotting the massive flood from: {max_flow_time.date()}")

window_start = max_flow_time - pd.Timedelta(days=2)
window_end = max_flow_time + pd.Timedelta(days=2)
flood_event = df_hourly.loc[window_start:window_end]

plt.figure(figsize=(12, 5))
plt.plot(flood_event.index, flood_event['Flow'], color='#D32F2F', lw=2.5, label='Hourly Flow')
plt.fill_between(flood_event.index, flood_event['Flow'], alpha=0.2, color='#D32F2F')
plt.axhline(5.0, color='gray', linestyle='--', label='Flood Threshold (5 m3/s)')
plt.title(f"Hydrograph: {basin_name} Stream - {max_flow_time.year}", fontsize=14, fontweight='bold')
plt.ylabel("Flow (m3/s)", fontsize=12)
plt.grid(True, alpha=0.3)
plt.legend()
plt.show()

# 8. Save to dynamic file
output_file = f'{basin_name}_Hourly_Grid.csv'
df_hourly.to_csv(output_file)
print(f"\nSuccess! Hourly grid saved to -> {output_file}")