import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import joblib
import os
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# ==========================================
# 1. Settings / Configuration
# ==========================================
DATASET_FILE = r'../organized_data/AegisEco_Alexander_ML_Dataset_v3.csv'

filename = os.path.basename(DATASET_FILE)
basin_name = filename.split('_')[1].capitalize()

BASIN_THRESHOLDS = {
    'Sorek': 5.0,
    'Dishon': 8.0,
    'Harod': 4.0,
    'Kishon': 50.0,
    'Zin': 1.0,
    'Keziv': 8.0,
    'Ayalon': 27.0,
    'Yarkon': 50.0,
    'Alexander': 50.0,
    'Taninim': 8.0,
}

FLOOD_THRESHOLD = BASIN_THRESHOLDS.get(basin_name, 5.0)

print(f"Booting up AegisEco {basin_name} v2 Agent...")
print(f"Flood alert threshold: {FLOOD_THRESHOLD} m3/s")

# ==========================================
# 2. Data Loading
# ==========================================
df = pd.read_csv(DATASET_FILE, index_col='DateTime', parse_dates=True)
print(f"Loaded {len(df):,} hours | {df.index[0].date()} → {df.index[-1].date()}")

# ==========================================
# 3. Separate Features and Target
# ==========================================
X = df.drop(columns=['Flow'])
y = df['Flow']

print(f"\nFeatures: {X.shape[1]}")
print(f"Target range: {y.min():.3f} → {y.max():.3f} m3/s")
print(f"Flood hours (>{FLOOD_THRESHOLD}): {(y > FLOOD_THRESHOLD).sum()} ({100*(y>FLOOD_THRESHOLD).mean():.2f}%)")

# ==========================================
# 4. Smart Split - Ensures floods in both train and test sets
# ==========================================
flood_times = y[y > FLOOD_THRESHOLD].index

# Identify distinct events (7-day gap = new event)
flood_series = pd.Series(flood_times)
gaps = flood_series.diff() > pd.Timedelta(days=7)
event_id = gaps.cumsum()
unique_events = event_id.nunique()

print(f"\nDistinct flood events found: {unique_events}")
for eid in range(unique_events):
    times = flood_times[event_id == eid]
    peak = y[times].max()
    print(f"  Event {eid+1}: {times[0].date()} → {times[-1].date()} | {len(times)}h | peak {peak:.1f} m3/s")

# Put the last 20% of events in the test set
n_test_events = max(2, int(unique_events * 0.2))
cutoff_event   = unique_events - n_test_events - 1
cutoff_time    = flood_times[event_id == cutoff_event][-1]

# In the training script, replace the smart split part with this:
# Manually define the split point to include major events in the test set
split_time = pd.Timestamp('2014-06-01')

X_train = X[X.index <= split_time]
X_test  = X[X.index >  split_time]
y_train = y[y.index <= split_time]
y_test  = y[y.index >  split_time]

print(f"Manual split at {split_time.date()}")
print(f"Train hours: {len(X_train):,} | Flood hours: {(y_train > FLOOD_THRESHOLD).sum()}")
print(f"Test hours:  {len(X_test):,}  | Flood hours: {(y_test  > FLOOD_THRESHOLD).sum()}")

# ==========================================
# 5. Train XGBoost
# ==========================================
print("\nTraining XGBoost Regressor...")

model = XGBRegressor(
    n_estimators=1000,
    learning_rate=0.05,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8,
    min_child_weight=5,    
    early_stopping_rounds=50,
    eval_metric='rmse',
    random_state=42,
    n_jobs=-1
)

model.fit(
    X_train, y_train,
    eval_set=[(X_test, y_test)],
    verbose=100
)

print(f"Best iteration: {model.best_iteration}")

# ==========================================
# 6. General Performance Evaluation
# ==========================================
y_pred = model.predict(X_test)

# Flow cannot be negative
y_pred = np.clip(y_pred, 0, None)  

mae  = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2   = r2_score(y_test, y_pred)

print(f"\n=== AegisEco {basin_name} v2: Performance Report ===")
print(f"MAE:  {mae:.4f} m3/s")
print(f"RMSE: {rmse:.4f} m3/s")
print(f"R²:   {r2:.4f}")

# ==========================================
# 7. Flood-Specific Performance Evaluation
# ==========================================
flood_mask = y_test > FLOOD_THRESHOLD

if flood_mask.sum() > 0:
    flood_mae    = mean_absolute_error(y_test[flood_mask], y_pred[flood_mask])
    true_pos     = (y_pred[flood_mask] > FLOOD_THRESHOLD).sum()
    false_neg    = flood_mask.sum() - true_pos
    false_pos    = ((y_pred > FLOOD_THRESHOLD) & ~flood_mask).sum()

    print(f"\n=== Flood-Specific Performance ===")
    print(f"Real flood hours:         {flood_mask.sum()}")
    print(f"Correctly flagged (TP):   {true_pos}  ({100*true_pos/flood_mask.sum():.1f}% recall)")
    print(f"Missed floods (FN):       {false_neg}")
    print(f"False alarms (FP):        {false_pos}")
    print(f"MAE during flood events:  {flood_mae:.4f} m3/s")
else:
    print("\nNo flood events in test set.")

# ==========================================
# 8. Feature Importance
# ==========================================
importances = pd.Series(
    model.feature_importances_,
    index=X.columns
).sort_values(ascending=True)

plt.figure(figsize=(12, 8))
colors = ['#C62828' if 'Flow' in c else '#1565C0' if 'Rain' in c or 'Basin' in c
          else '#2E7D32' if 'Soil' in c or 'Rolling' in c
          else '#6A1B9A' for c in importances.index]
importances.plot(kind='barh', color=colors)
plt.title(f'AegisEco {basin_name} v2: Feature Importance\n(red=flow, blue=rain, green=soil, purple=seasonal)')
plt.xlabel('Importance Score')
plt.tight_layout()
plt.show()

# ==========================================
# 9. General Plot: Prediction vs. Reality
# ==========================================
plt.figure(figsize=(14, 5))
plt.plot(y_test.index, y_test.values, label='Actual Flow',    color='steelblue', alpha=0.8, linewidth=1)
plt.plot(y_test.index, y_pred,        label='Predicted Flow', color='orange',    alpha=0.8, linewidth=1)
plt.axhline(y=FLOOD_THRESHOLD, color='red', linestyle='--', label=f'Flood ({FLOOD_THRESHOLD} m3/s)')
plt.title(f'AegisEco {basin_name} v2: Predicted vs Actual Flow')
plt.ylabel('Flow (m3/s)')
plt.xlabel('Date')
plt.legend()
plt.tight_layout()
plt.show()

# ==========================================
# 10. Zoom in on each flood event separately
# ==========================================
if flood_mask.sum() > 0:
    flood_event_times = y_test.index[flood_mask]
    flood_series_test = pd.Series(flood_event_times)
    gaps_test = flood_series_test.diff() > pd.Timedelta(days=7)
    event_id_test = gaps_test.cumsum()

    for eid in event_id_test.unique():
        event_times = flood_event_times[event_id_test == eid]
        window = pd.Timedelta(hours=72)
        zoom_start = event_times[0] - window
        zoom_end   = event_times[-1] + window
        zoom_mask  = (y_test.index >= zoom_start) & (y_test.index <= zoom_end)

        peak_actual = y_test[zoom_mask].max()
        peak_pred   = y_pred[zoom_mask].max()

        fig, ax = plt.subplots(figsize=(14, 5))
        ax.plot(y_test.index[zoom_mask], y_test.values[zoom_mask],
                label='Actual Flow', color='steelblue', linewidth=2)
        ax.plot(y_test.index[zoom_mask], y_pred[zoom_mask],
                label='Predicted Flow', color='orange', linewidth=2)
        ax.axhline(y=FLOOD_THRESHOLD, color='red', linestyle='--',
                   label=f'Flood threshold ({FLOOD_THRESHOLD} m3/s)')
        ax.set_title(f'AegisEco {basin_name}: Flood Event {eid+1} | '
                     f'Actual peak: {peak_actual:.1f} | Predicted peak: {peak_pred:.1f} m3/s')
        ax.set_ylabel('Flow (m3/s)')
        ax.set_xlabel('Date')
        ax.legend()
        plt.tight_layout()
        plt.show()

# ==========================================
# 11. Save the Model
# ==========================================
# Extract rain station names from the columns
rain_station_cols = [c for c in X.columns if c.startswith('Rain_') and 'lag' not in c and 'Mean' not in c and 'Max' not in c and 'Std' not in c and 'Acc' not in c]
station_names = [c.replace('Rain_', '') for c in rain_station_cols]

agent_brain = {
    'basin_name':     basin_name,
    'model':          model,
    'model_type':     'xgboost_regressor',
    'flood_stage_m3s': FLOOD_THRESHOLD,
    'feature_names':  X.columns.tolist(),
    'station_names':  station_names,
    'test_r2':        r2,
    'test_mae':       mae,
    'best_iteration': model.best_iteration,
}

output_name = f'model_{basin_name.lower()}_v3.pkl'
joblib.dump(agent_brain, output_name)
print(f"\nSaved -> {output_name}")
print(f"Stations embedded: {station_names}")