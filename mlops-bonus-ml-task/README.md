# XGBoost ML Pipeline with Feature Engineering and MLflow Tracking

This section demonstrates a full machine learning pipeline using XGBoost for binary classification, complete with:

- Data cleaning and merging
- Custom feature engineering
- Class imbalance handling (SMOTE)
- Hyperparameter tuning (GridSearchCV)
- MLflow tracking and model logging
- Final model export
- Visualizations (feature importance, confusion matrix)

---

## Project Structure

.
├── data/
│   ├── customer_clean.csv
│   └── payment_clean.csv
├── models/
│   └── xgb_model.pkl
├── notebooks/
│   └── model_training.ipynb
├── scripts/
│   └── xgb_model_training.py
├── mlruns/                # MLflow runs (these artifacts are copied from local execution of the jupyter notebook)
├── requirements.txt
└── README.md

---

## Setup Instructions

### 1. Create and activate a virtual environment

python3 -m venv prj_mlflow
source prj_mlflow/bin/activate

### 2. Install dependencies
pip install -r requirements.txt

### 3. Run the training script
python scripts/xgb_model_training.py

This script will:
	•	Merge and process the customer and payment datasets
	•	Generate features like debt_ratio, mean_ovd, and is_over_limit
	•	Train an XGBoost model with SMOTE and StandardScaler in a pipeline
	•	Tune hyperparameters using GridSearchCV
	•	Log the model and metrics with MLflow
	•	Save the final model in models/xgb_model.pkl

### 4. Explore the notebook

jupyter notebook notebooks/model_training.ipynb
<img width="1167" height="977" alt="image" src="https://github.com/user-attachments/assets/9069392a-b2f3-4699-8bd0-eb8b1ec7068a" />

### 5. MLflow UI

mlflow ui --backend-store-uri ./mlruns and open in http://localhost:5000
<img width="1913" height="571" alt="image" src="https://github.com/user-attachments/assets/9f093306-ab63-4660-819d-ff33714e67ce" />

