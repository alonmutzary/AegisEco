import pandas as pd
import numpy as np
import os

# ==========================================
# 1. Settings
# ==========================================
RAIN_FILES = [
    r'Jerusalem_Center_Hourly_Rain_Features.csv',
    r'Bet_Jamal_Hourly_Rain_Features.csv',
    r'Hefez_Hayim_Hourly_Rain_Features.csv',
]

FLOW_FILE  = r'Sorek_Hourly_Grid.csv'
basin_name = os.path.basename(FLOW_FILE).split('_')[0].capitalize()

print(f"[{basin_name}] Station-Agnostic Feature Engineering")
print(f"[{basin_name}] Stations: {len(RAIN_FILES)}")

# ==========================================
# 2. Load all stations into a list
# ==========================================
df_flow    = pd.read_csv(FLOW_FILE, index_col='DateTime', parse_dates=True)
station_dfs = []

for rain_file in RAIN_FILES:
    station_id = os.path.basename(rain_file).replace('_Hourly_Rain_Features.csv', '')
    df_s = pd.read_csv(rain_file, index_col='DateTime', parse_dates=True)
    df_s.columns = ['Rain', 'Intensity']  
    df_s['_station'] = station_id         
    station_dfs.append(df_s)
    print(f"  Loaded: {station_id} ({len(df_s):,} hours)")

# ==========================================
# 3. Calculate spatial statistics per hour
# ==========================================
# Aligns all stations to the same time index
rain_arrays      = pd.concat([d['Rain']      for d in station_dfs], axis=1).fillna(0)
intensity_arrays = pd.concat([d['Intensity'] for d in station_dfs], axis=1).fillna(0)

# How many stations reported rain > 0 each hour (spatial coverage metric)
active_stations  = (rain_arrays > 0).sum(axis=1)

df_spatial = pd.DataFrame(index=rain_arrays.index)
df_spatial['Basin_Rain_Mean']        = rain_arrays.mean(axis=1)
df_spatial['Basin_Rain_Max']         = rain_arrays.max(axis=1)
df_spatial['Basin_Rain_Std']         = rain_arrays.std(axis=1).fillna(0)
df_spatial['Basin_Rain_Count']       = active_stations         
df_spatial['Basin_Intensity_Max']    = intensity_arrays.max(axis=1)
df_spatial['Basin_Intensity_Mean']   = intensity_arrays.mean(axis=1)

# ==========================================
# 4. Merge with flow
# ==========================================
df = pd.merge(df_spatial, df_flow, left_index=True, right_index=True, how='inner')

# ==========================================
# 5. Autoregressive flow features
# ==========================================
for lag in [1, 2, 3, 6, 12, 24]:
    df[f'Flow_lag{lag}h'] = df['Flow'].shift(lag)

df['Flow_Rate_of_Change'] = df['Flow'].diff()
df['Flow_Is_Active']      = (df['Flow'].shift(1) > 0.1).astype(int)

# ==========================================
# 6. Soil Moisture
# ==========================================
df['Soil_Moisture_EWM']  = df['Basin_Rain_Mean'].ewm(alpha=0.02, adjust=False).mean().shift(1)
df['Rolling_Rain_24h']   = df['Basin_Rain_Mean'].rolling(24).sum().shift(1)
df['Rolling_Rain_72h']   = df['Basin_Rain_Mean'].rolling(72).sum().shift(1)
df['Rolling_Rain_168h']  = df['Basin_Rain_Mean'].rolling(168).sum().shift(1)

# ==========================================
# 7. Rain Lags (Generic, not by station)
# ==========================================
for lag in range(1, 7):
    df[f'Basin_Rain_lag{lag}h']      = df['Basin_Rain_Mean'].shift(lag)
    df[f'Basin_Intensity_lag{lag}h'] = df['Basin_Intensity_Max'].shift(lag)

df['Rain_Acceleration'] = df['Basin_Rain_Mean'].diff()

# ==========================================
# 8. Seasonality
# ==========================================
df['Month_Sin']       = np.sin(2 * np.pi * df.index.month / 12)
df['Month_Cos']       = np.cos(2 * np.pi * df.index.month / 12)
df['Hour_Sin']        = np.sin(2 * np.pi * df.index.hour / 24)
df['Hour_Cos']        = np.cos(2 * np.pi * df.index.hour / 24)
df['Is_Early_Winter'] = df.index.month.isin([10, 11]).astype(int)
df['Is_Peak_Winter']  = df.index.month.isin([12, 1, 2]).astype(int)
df['Is_Summer']       = df.index.month.isin([6, 7, 8, 9]).astype(int)

# ==========================================
# FILTER: Remove non-rainfall flow events
# ==========================================
print(f"[{basin_name}] Filtering non-rainfall flow events...")

original_len = len(df)

# Flags rows with high flow but zero rain in the 24-hour window prior
FLOOD_THRESHOLD_FILTER = 4.0  # Same threshold as in settings

# Cumulative rain 24 hours back (not shifted, because we want to include the current hour)
rolling_rain_check = df['Basin_Rain_Mean'].rolling(window=24).sum()

# Identify suspicious rows: flow above threshold + less than 1mm rain in 24 hours
suspicious_mask = (df['Flow'] > FLOOD_THRESHOLD_FILTER) & (rolling_rain_check < 1.0)

# Print what is being filtered (full transparency)
suspicious_rows = df[suspicious_mask]
if len(suspicious_rows) > 0:
    print(f"  Removing {len(suspicious_rows)} suspicious non-rainfall flood hours:")
    for ts in suspicious_rows.index:
        print(f"    {ts} | Flow: {df.loc[ts, 'Flow']:.1f} m3/s | "
              f"Rain 24h: {rolling_rain_check.loc[ts]:.1f}mm")

# Method: Do not delete the rows completely - just reset the flow values to NaN
# This preserves time continuity for calculating lags
df.loc[suspicious_mask, 'Flow'] = np.nan

# After resetting, need to recalculate all flow features
print(f"  Recalculating flow features after filtering...")

for lag in [1, 2, 3, 6, 12, 24]:
    df[f'Flow_lag{lag}h'] = df['Flow'].shift(lag)

df['Flow_Rate_of_Change'] = df['Flow'].diff()
df['Flow_Is_Active']      = (df['Flow'].shift(1) > 0.1).astype(int)

# Now delete rows with NaN (including the reset rows)
df = df.dropna()

print(f"  Rows before: {original_len:,} | After: {len(df):,} | "
      f"Removed: {original_len - len(df):,}")

remaining_summer = df[
    (df['Flow'] > FLOOD_THRESHOLD_FILTER) & 
    df.index.month.isin([4,5,6,7,8,9])
]
print(f"  Remaining summer flood hours after filter: {len(remaining_summer)} "
      f"(should be 0)")

# ==========================================
# 9. Cleanup
# ==========================================
df = df.dropna()

print(f"\n=== {basin_name} Station-Agnostic Dataset ===")
print(f"Hours: {len(df):,} | Features: {len(df.columns)-1}")
print(f"Range: {df.index[0].date()} → {df.index[-1].date()}")
print("\nFeatures (these never change regardless of stations):")
for col in df.columns:
    if col != 'Flow':
        print(f"  - {col}")

output_file = f'AegisEco_{basin_name}_ML_Dataset_v3.csv'
df.to_csv(output_file)
print(f"\nSaved → {output_file}")