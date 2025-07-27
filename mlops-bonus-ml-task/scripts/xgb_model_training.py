import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
import joblib
import xgboost as xgb

from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import roc_auc_score, classification_report, confusion_matrix, ConfusionMatrixDisplay
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline as ImbPipeline

# =========================
# 1. Load and prepare data
# =========================
customer_df = pd.read_csv("data/customer_clean.csv")
payment_df = pd.read_csv("data/payment_clean.csv")

df = customer_df.merge(payment_df, on="id", how="left")
df.fillna(df.median(numeric_only=True), inplace=True)

# =========================
# 2. Feature engineering
# =========================
df["debt_ratio"] = df["new_balance"] / (df["credit_limit"] + 1e-5)
df["is_over_limit"] = (df["new_balance"] > df["credit_limit"]).astype(int)
df["mean_ovd"] = df[["OVD_t1", "OVD_t2", "OVD_t3"]].mean(axis=1)

drop_cols = ["id", "label", "update_date", "report_date"]
X = df.drop(columns=drop_cols, errors='ignore')
y = df["label"]

X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y, test_size=0.2, random_state=42)

# =========================
# 3. Define pipeline and grid search
# =========================
pipeline = ImbPipeline(steps=[
    ("scaler", StandardScaler()),
    ("smote", SMOTE(random_state=42)),
    ("xgb", xgb.XGBClassifier(use_label_encoder=False, eval_metric="logloss", random_state=42))
])

param_grid = {
    "xgb__n_estimators": [100, 200],
    "xgb__max_depth": [3, 5],
    "xgb__learning_rate": [0.05, 0.1],
    "xgb__subsample": [0.8, 1.0]
}

grid = GridSearchCV(pipeline, param_grid=param_grid, cv=3, scoring='roc_auc', verbose=2, n_jobs=-1)
grid.fit(X_train, y_train)

# =========================
# 4. Evaluation & logging MLflow
# =========================
y_pred = grid.predict(X_test)
y_proba = grid.predict_proba(X_test)[:, 1]
roc_auc = roc_auc_score(y_test, y_proba)

mlflow.set_tracking_uri("file://./mlruns")
mlflow.set_experiment("xgb_experiment")

with mlflow.start_run(run_name="xgboost_pipeline_gridsearch"):
    mlflow.log_params(grid.best_params_)
    mlflow.log_metric("roc_auc", roc_auc)
    mlflow.sklearn.log_model(grid.best_estimator_, "xgb_model")

# =========================
# 5. Save model & plot results
# =========================
print("Best params:", grid.best_params_)
print("ROC-AUC:", roc_auc)
print("Classification Report:\n", classification_report(y_test, y_pred))

ConfusionMatrixDisplay(confusion_matrix=confusion_matrix(y_test, y_pred)).plot()
plt.title("Confusion Matrix")
plt.show()

model_final = grid.best_estimator_.named_steps["xgb"]
xgb.plot_importance(model_final, max_num_features=10, importance_type="gain")
plt.title("Top 10 Feature Importances")
plt.show()

joblib.dump(grid.best_estimator_, "models/xgb_model.pkl")
print("Model saved in în models/xgb_model.pkl")
