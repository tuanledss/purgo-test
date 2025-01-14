import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import mlflow
import mlflow.sklearn

# Load and preprocess data
np.random.seed(42)
n_records = 30
fixed_acidity = np.random.uniform(5, 10, n_records)
volatile_acidity = np.random.uniform(0.1, 0.5, n_records)
citric_acid = np.random.uniform(0, 0.5, n_records)
residual_sugar = np.random.uniform(1, 10, n_records)
chlorides = np.random.uniform(0.01, 0.1, n_records)
free_sulfur_dioxide = np.random.uniform(5, 50, n_records)
total_sulfur_dioxide = np.random.uniform(20, 150, n_records)
density = np.random.uniform(0.990, 1.000, n_records)
pH = np.random.uniform(2.8, 3.5, n_records)
sulphates = np.random.uniform(0.4, 1.0, n_records)
alcohol = np.random.uniform(8, 14, n_records)
quality = np.random.randint(3, 9, n_records)
high_quality = (quality >= 6).astype(int)

data = pd.DataFrame({
    'fixed_acidity': fixed_acidity,
    'volatile_acidity': volatile_acidity,
    'citric_acid': citric_acid,
    'residual_sugar': residual_sugar,
    'chlorides': chlorides,
    'free_sulfur_dioxide': free_sulfur_dioxide,
    'total_sulfur_dioxide': total_sulfur_dioxide,
    'density': density,
    'pH': pH,
    'sulphates': sulphates,
    'alcohol': alcohol,
    'quality': quality,
    'high_quality': high_quality
})

train_data = data.sample(frac=0.7, random_state=42)
remaining_data = data.drop(train_data.index)
validation_data = remaining_data.sample(frac=0.5, random_state=42)
test_data = remaining_data.drop(validation_data.index)

# Train the model
X_train = train_data.drop(columns=['quality', 'high_quality'])
y_train = train_data['high_quality']
model = RandomForestClassifier(random_state=42)
model.fit(X_train, y_train)

# Validate and test the model
X_val = validation_data.drop(columns=['quality', 'high_quality'])
y_val = validation_data['high_quality']
val_predictions = model.predict(X_val)
val_accuracy = accuracy_score(y_val, val_predictions)

X_test = test_data.drop(columns=['quality', 'high_quality'])
y_test = test_data['high_quality']
test_predictions = model.predict(X_test)
test_accuracy = accuracy_score(y_test, test_predictions)

# Track experiments
mlflow.set_experiment("Wine Quality Classification")
with mlflow.start_run():
    mlflow.sklearn.log_model(model, "model")
    mlflow.log_metric("validation_accuracy", val_accuracy)
    mlflow.log_metric("test_accuracy", test_accuracy)

# Output results
print(f"Validation Accuracy: {val_accuracy}")
print(f"Test Accuracy: {test_accuracy}")
