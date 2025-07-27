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

<img width="422" height="365" alt="image" src="https://github.com/user-attachments/assets/03a6bfc3-b73a-4dcf-80c8-872cd39772b4" />


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

## Model Evaluation

The final XGBoost model achieved a **ROC-AUC score of ~0.93** on the test set, indicating strong ability to distinguish between classes.  
The **confusion matrix** shows a good balance between precision and recall, with most positive and negative samples correctly classified.  
We used **SMOTE** to handle class imbalance and performed **GridSearchCV** for hyperparameter tuning.  
Feature importance analysis revealed that variables like `mean_ovd`, `debt_ratio`, and `credit_limit` had the strongest predictive power.

*Note: This model was trained on a synthetic dataset. Real-world performance may vary.*
