import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import mlflow
import mlflow.sklearn
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
CSV_FILE_PATH = '/dbfs/FileStore/winequality-white.csv'
QUALITY_THRESHOLD = 7
TEST_SIZE = 0.15
VALIDATION_SIZE = 0.15
RANDOM_STATE = 42

# Data Loader
def load_data(file_path):
    try:
        df = pd.read_csv(file_path)
        logger.info("Data loaded successfully.")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

# Data Preprocessor
def preprocess_data(df):
    try:
        df['high_quality'] = df['quality'] >= QUALITY_THRESHOLD
        train, temp = train_test_split(df, test_size=TEST_SIZE + VALIDATION_SIZE, random_state=RANDOM_STATE)
        validation, test = train_test_split(temp, test_size=TEST_SIZE / (TEST_SIZE + VALIDATION_SIZE), random_state=RANDOM_STATE)
        logger.info("Data preprocessed successfully.")
        return train, validation, test
    except Exception as e:
        logger.error(f"Error in data preprocessing: {e}")
        raise

# Model Trainer
def train_model(train_df):
    try:
        X_train = train_df.drop(columns=['quality', 'high_quality'])
        y_train = train_df['high_quality']
        model = RandomForestClassifier(random_state=RANDOM_STATE)
        model.fit(X_train, y_train)
        logger.info("Model trained successfully.")
        return model
    except Exception as e:
        logger.error(f"Error in model training: {e}")
        raise

# Model Validator and Tester
def evaluate_model(model, validation_df, test_df):
    try:
        X_val = validation_df.drop(columns=['quality', 'high_quality'])
        y_val = validation_df['high_quality']
        X_test = test_df.drop(columns=['quality', 'high_quality'])
        y_test = test_df['high_quality']

        val_predictions = model.predict(X_val)
        test_predictions = model.predict(X_test)

        val_accuracy = accuracy_score(y_val, val_predictions)
        test_accuracy = accuracy_score(y_test, test_predictions)
        test_precision = precision_score(y_test, test_predictions)
        test_recall = recall_score(y_test, test_predictions)
        test_f1 = f1_score(y_test, test_predictions)

        logger.info(f"Validation Accuracy: {val_accuracy}")
        logger.info(f"Test Accuracy: {test_accuracy}")
        logger.info(f"Test Precision: {test_precision}")
        logger.info(f"Test Recall: {test_recall}")
        logger.info(f"Test F1 Score: {test_f1}")

        return test_accuracy, test_precision, test_recall, test_f1
    except Exception as e:
        logger.error(f"Error in model evaluation: {e}")
        raise

# Experiment Tracker
def log_experiment(model, test_accuracy, test_precision, test_recall, test_f1):
    try:
        with mlflow.start_run():
            mlflow.sklearn.log_model(model, "model")
            mlflow.log_metric("test_accuracy", test_accuracy)
            mlflow.log_metric("test_precision", test_precision)
            mlflow.log_metric("test_recall", test_recall)
            mlflow.log_metric("test_f1", test_f1)
            logger.info("Experiment logged successfully.")
    except Exception as e:
        logger.error(f"Error in logging experiment: {e}")
        raise

# Main function to execute the pipeline
def main():
    try:
        df = load_data(CSV_FILE_PATH)
        train_df, validation_df, test_df = preprocess_data(df)
        model = train_model(train_df)
        test_accuracy, test_precision, test_recall, test_f1 = evaluate_model(model, validation_df, test_df)
        if test_accuracy >= 0.8:
            log_experiment(model, test_accuracy, test_precision, test_recall, test_f1)
        else:
            logger.warning("Model did not achieve the required accuracy threshold.")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")

if __name__ == "__main__":
    main()
