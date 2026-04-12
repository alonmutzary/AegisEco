import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

FLOW_FILE = 'Sorek_Hourly_Grid.csv'  
basin_name = os.path.basename(FLOW_FILE).split('_')[0].capitalize()

df_flow = pd.read_csv(FLOW_FILE, index_col='DateTime', parse_dates=True)
flow = df_flow['Flow']

# --- Basic statistics ---
print("=== Flow Distribution ===")
print(f"Mean:        {flow.mean():.2f} m³/s")
print(f"Median:      {flow.median():.2f} m³/s")
print(f"95th pctile: {flow.quantile(0.95):.2f} m³/s")
print(f"99th pctile: {flow.quantile(0.99):.2f} m³/s")
print(f"99.9th pctl: {flow.quantile(0.999):.2f} m³/s")
print(f"Max ever:    {flow.max():.2f} m³/s")

# --- How many flood EVENTS at different thresholds? ---
# An "event" = consecutive hours above threshold (not individual hours)
print("\n=== Flood Events at Different Thresholds ===")
for thresh in [3, 5, 8, 10, 15, 20, 30, 50]:
    above = (flow > thresh).astype(int)
    # Count transitions from 0→1 = number of distinct events
    events = (above.diff() == 1).sum()
    hours = above.sum()
    print(f"  {thresh:>5} m³/s:  {events:>4} events,  {hours:>5} hours above threshold")

# --- Visual: flow duration curve ---
sorted_flow = np.sort(flow)[::-1]
exceedance = np.arange(1, len(sorted_flow)+1) / len(sorted_flow) * 100

plt.figure(figsize=(10, 5))
plt.semilogy(exceedance, sorted_flow)
plt.xlabel('% of time flow exceeded')
plt.ylabel('Flow (m³/s) — log scale')
plt.title(f'Flow Duration Curve — {basin_name} Basin')
plt.axhline(y=8, color='red', linestyle='--', label='Current threshold (8 m³/s)')
plt.grid(True, alpha=0.3)
plt.legend()
plt.tight_layout()
plt.show()