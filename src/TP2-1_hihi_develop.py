import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load the dataset
csv_file_path = '/dbfs/FileStore/winequality-white.csv'
df = pd.read_csv(csv_file_path)

# Convert 'quality' to binary 'high_quality'
df['high_quality'] = (df['quality'] >= 7).astype(int)

# Split the dataset into training, validation, and test sets
train_df, temp_df = train_test_split(df, test_size=0.3, random_state=42)
validation_df, test_df = train_test_split(temp_df, test_size=0.5, random_state=42)

# Features and labels
features = df.columns.drop(['quality', 'high_quality'])
X_train = train_df[features]
y_train = train_df['high_quality']
X_validation = validation_df[features]
y_validation = validation_df['high_quality']
X_test = test_df[features]
y_test = test_df['high_quality']

# Train the Random Forest model
model = RandomForestClassifier(random_state=42)
model.fit(X_train, y_train)

# Validate the model
validation_predictions = model.predict(X_validation)
validation_accuracy = accuracy_score(y_validation, validation_predictions)
logger.info(f"Validation Accuracy: {validation_accuracy}")

# Test the model
test_predictions = model.predict(X_test)
test_accuracy = accuracy_score(y_test, test_predictions)
logger.info(f"Test Accuracy: {test_accuracy}")

# Check if the model meets the acceptance criteria
if test_accuracy >= 0.8:
    logger.info("Model meets the acceptance criteria.")
else:
    logger.error("Model does not meet the acceptance criteria.")

# Log the experiment in MLflow
with mlflow.start_run():
    mlflow.sklearn.log_model(model, "model")
    mlflow.log_metric("validation_accuracy", validation_accuracy)
    mlflow.log_metric("test_accuracy", test_accuracy)
    mlflow.log_params(model.get_params())

    # Register the model if it meets the acceptance criteria
    if test_accuracy >= 0.8:
        mlflow.register_model("runs:/{}/model".format(mlflow.active_run().info.run_id), "WineQualityModel")

logger.info("Pipeline execution completed.")
