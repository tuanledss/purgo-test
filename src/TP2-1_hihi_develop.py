import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import mlflow
import mlflow.sklearn
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load data
def load_data(file_path):
    try:
        df = pd.read_csv(file_path)
        logger.info("Data loaded successfully.")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

# Preprocess data
def preprocess_data(df):
    try:
        df['high_quality'] = df['quality'].apply(lambda x: 1 if x >= 7 else 0)
        df = df.drop(columns=['quality'])
        logger.info("Data preprocessed successfully.")
        return df
    except Exception as e:
        logger.error(f"Error in data preprocessing: {e}")
        raise

# Train model
def train_model(X_train, y_train):
    try:
        model = RandomForestClassifier()
        model.fit(X_train, y_train)
        logger.info("Model trained successfully.")
        return model
    except Exception as e:
        logger.error(f"Error in model training: {e}")
        raise

# Evaluate model
def evaluate_model(model, X_test, y_test):
    try:
        predictions = model.predict(X_test)
        accuracy = accuracy_score(y_test, predictions)
        logger.info(f"Model accuracy: {accuracy}")
        return accuracy
    except Exception as e:
        logger.error(f"Error in model evaluation: {e}")
        raise

# Main pipeline
def main_pipeline(file_path):
    try:
        # Load and preprocess data
        df = load_data(file_path)
        df = preprocess_data(df)
        X = df.drop(columns=['high_quality'])
        y = df['high_quality']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
        
        # Train model
        model = train_model(X_train, y_train)
        
        # Evaluate model
        accuracy = evaluate_model(model, X_test, y_test)
        
        # Log experiment with MLflow
        with mlflow.start_run():
            mlflow.log_param("model_type", "RandomForest")
            mlflow.log_metric("accuracy", accuracy)
            mlflow.sklearn.log_model(model, "model")
            logger.info("Experiment logged in MLflow.")
        
        # Check acceptance criteria
        if accuracy >= 0.8:
            logger.info("Model meets the acceptance criteria.")
        else:
            logger.warning("Model does not meet the acceptance criteria.")
        
    except Exception as e:
        logger.error(f"Error in main pipeline: {e}")
        raise

# Run the pipeline
if __name__ == "__main__":
    file_path = '/dbfs/FileStore/winequality-white.csv'
    main_pipeline(file_path)
