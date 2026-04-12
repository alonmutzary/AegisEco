import pandas as pd
import os

# ==========================================
# 1. Settings and File List (Insert as many files as you want here)
# ==========================================
FILE_PATHS = [
    r'../ims_data/hefez_hayim_2010_2018.csv',
    r'../ims_data/hefez_hayim_2018_2019.csv'
]

# Smart extraction of the station name from the first file
first_filename = os.path.basename(FILE_PATHS[0]).replace('.csv', '')
parts = first_filename.split('_')
# Takes all parts of the name except the last two years, and formats (e.g., "Jerusalem Center")
station_name = " ".join(parts[:-2]).title() 
if not station_name: 
    station_name = parts[0].title()

print(f"Loading and combining IMS rainfall files for: {station_name}...")

# ==========================================
# 2. Load and Concatenate Files
# ==========================================
df_list = []
for file in FILE_PATHS:
    df_list.append(pd.read_csv(file))

# Concatenate all files into one long table
df_rain = pd.concat(df_list, ignore_index=True)

# ==========================================
# 3. Column Arrangement and Cleaning
# ==========================================
df_rain.columns = ['Station', 'DateTime', 'Rainfall']

# Convert the rain column to numeric (fixes weird characters IMS sometimes inserts)
df_rain['Rainfall'] = pd.to_numeric(df_rain['Rainfall'], errors='coerce').fillna(0)

print("Parsing dates (this might take a few seconds)...")
df_rain['DateTime'] = pd.to_datetime(df_rain['DateTime'], format='%d/%m/%Y %H:%M', errors='coerce')

df_rain = df_rain.dropna(subset=['DateTime'])
df_rain = df_rain.set_index('DateTime')

# ==========================================
# 4. Resampling - Create Hourly Features
# ==========================================
print("Resampling to hourly grid and extracting features...")

df_hourly_rain = df_rain.resample('1h').agg(
    Hourly_Rain_Sum=('Rainfall', 'sum'),
    Max_10min_Intensity=('Rainfall', 'max')
)

df_hourly_rain = df_hourly_rain.fillna(0)

# ==========================================
# 5. Results Check and Dynamic Printing
# ==========================================
print(f"\n=== AegisEco: {station_name} Rain Preprocessing ===")
print(f"Total hourly rows (Grid): {len(df_hourly_rain):,}")
print(f"Max hourly rain recorded: {df_hourly_rain['Hourly_Rain_Sum'].max()} mm")
print(f"Max 10-min intensity recorded: {df_hourly_rain['Max_10min_Intensity'].max()} mm")

# Save to a dynamic file
# Replaces spaces with underscores for the filename
safe_station_name = station_name.replace(" ", "_")
output_file = f'{safe_station_name}_Hourly_Rain_Features.csv'

df_hourly_rain.to_csv(output_file)
print(f"\nSuccess! Hourly rainfall features saved to -> {output_file}")