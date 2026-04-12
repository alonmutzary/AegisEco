import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import joblib
import os
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, precision_recall_curve

# ==========================================
# 1. Dynamic Settings and Configuration
# ==========================================
# Change only this path when you train a new basin!
DATASET_FILE = r'../organized_data/AegisEco_Harod_ML_Dataset.csv' 

filename = os.path.basename(DATASET_FILE)
basin_name = filename.split('_')[1].capitalize() # Extracts 'Sorek', 'Dishon', etc.

# Thresholds dictionary (Flood Stage) for each basin. This replaces the hardcoding!
BASIN_THRESHOLDS = {
    'Sorek': 5.0,
    'Dishon': 8.0, 
    'Harod': 3.0,  
    'Zin': 1.0,
    'Ayalon': 60.0
}

FLOOD_THRESHOLD = BASIN_THRESHOLDS.get(basin_name, 5.0)

print(f"Booting up AegisEco {basin_name} Agent...")
print(f"Loaded config: Flood threshold set to {FLOOD_THRESHOLD} m3/s")

# ==========================================
# 2. Data Loading
# ==========================================
df = pd.read_csv(DATASET_FILE, index_col='DateTime', parse_dates=True)



# ==========================================
# 3. Define "What is a flood" (Target Variable)
# ==========================================
df['is_flood'] = (df['Flow'] > FLOOD_THRESHOLD).astype(int)

# ==========================================
# 4. Separate Features and Target
# ==========================================
X = df.drop(columns=['Flow', 'is_flood'])
y = df['is_flood']

# ==========================================
# 5. Chronological Time-Based Split
# ==========================================
split_idx = int(len(df) * 0.8) 
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

print(f"\nTraining on {len(X_train):,} hours (Past)")
print(f"Testing on {len(X_test):,} hours (Future)")
print(f"Floods in test set: {y_test.sum()} hours")

# ==========================================
# 6. Train the Model (The AI Brain)
# ==========================================
print("\nTraining Random Forest Classifier...")
model = RandomForestClassifier(n_estimators=200, max_depth=10, class_weight='balanced', random_state=42, n_jobs=-1)
model.fit(X_train, y_train)

# ==========================================
# 7. Balanced Operational Threshold
# ==========================================
y_proba = model.predict_proba(X_test)[:, 1]

optimal_threshold = 0.50 

y_pred = (y_proba >= optimal_threshold).astype(int)

# ==========================================
# 8. Performance Report
# ==========================================
print(f"\n=== AegisEco {basin_name} Agent: Performance Report ===")
print(f"Optimal Decision Threshold: {optimal_threshold:.3f}")

print("\nConfusion Matrix:")
print(confusion_matrix(y_test, y_pred))

print("\nClassification Report:")
print(classification_report(y_test, y_pred, target_names=['Normal (Dry)', 'Flood Alert'], zero_division=0))

# ==========================================
# 9. Feature Importance (Graph)
# ==========================================
importances = pd.Series(model.feature_importances_, index=X.columns).sort_values(ascending=True)
plt.figure(figsize=(10, 6))
importances.plot(kind='barh', color='#7B1FA2')
plt.title(f'AegisEco: What Triggers a Flood in {basin_name} Basin?')
plt.xlabel('Importance Score')
plt.tight_layout()
plt.show()

# # ==========================================
# # 10. Save "The Brain"
# # ==========================================
print(f"\nSaving the trained model and metadata for {basin_name} autonomous agent...")

agent_brain = {
    'basin_name': basin_name,
    'model': model,
    'optimal_threshold': optimal_threshold,
    'flood_stage_m3s': FLOOD_THRESHOLD,
    'feature_names': X.columns.tolist() 
}

output_model_name = f'model_{basin_name.lower()}_v1.pkl'
joblib.dump(agent_brain, output_model_name)
print(f"Success! Agent brain saved as '{output_model_name}'")