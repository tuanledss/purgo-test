import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
import mlflow
import mlflow.sklearn

# Load and preprocess data
def load_and_preprocess_data():
    # Generate synthetic wine quality data
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

    wine_data = pd.DataFrame({
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

    train_data, temp_data = train_test_split(wine_data, test_size=0.3, random_state=42)
    validation_data, test_data = train_test_split(temp_data, test_size=0.5, random_state=42)

    return train_data, validation_data, test_data

# Train the model
def train_model(train_data, features, target):
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(train_data[features], train_data[target])
    return model

# Validate and test the model
def evaluate_model(model, validation_data, test_data, features, target):
    val_predictions = model.predict(validation_data[features])
    val_accuracy = accuracy_score(validation_data[target], val_predictions)

    test_predictions = model.predict(test_data[features])
    test_accuracy = accuracy_score(test_data[target], test_predictions)

    return val_accuracy, test_accuracy

# Track experiments
def log_experiment(model, test_accuracy):
    mlflow.set_experiment("Wine Quality Classification")
    with mlflow.start_run():
        mlflow.log_param("n_estimators", 100)
        mlflow.log_metric("test_accuracy", test_accuracy)
        mlflow.sklearn.log_model(model, "model")

# Main pipeline
def main():
    train_data, validation_data, test_data = load_and_preprocess_data()
    features = ['fixed_acidity', 'volatile_acidity', 'citric_acid', 'residual_sugar', 
                'chlorides', 'free_sulfur_dioxide', 'total_sulfur_dioxide', 'density', 
                'pH', 'sulphates', 'alcohol']
    target = 'high_quality'

    model = train_model(train_data, features, target)
    val_accuracy, test_accuracy = evaluate_model(model, validation_data, test_data, features, target)

    if test_accuracy >= 0.8:
        log_experiment(model, test_accuracy)
        print(f"Model registered with test accuracy: {test_accuracy}")
    else:
        print(f"Model did not meet accuracy requirement. Test accuracy: {test_accuracy}")

if __name__ == "__main__":
    main()
