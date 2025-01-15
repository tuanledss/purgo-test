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

def load_data(file_path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(file_path)
        logger.info("Data loaded successfully.")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def preprocess_data(df: pd.DataFrame) -> tuple:
    try:
        df['high_quality'] = (df['quality'] >= 7).astype(int)
        train, temp = train_test_split(df, test_size=0.3, random_state=42)
        val, test = train_test_split(temp, test_size=0.5, random_state=42)
        logger.info("Data preprocessed successfully.")
        return train, val, test
    except Exception as e:
        logger.error(f"Error in data preprocessing: {e}")
        raise

def train_model(train_df: pd.DataFrame) -> RandomForestClassifier:
    try:
        features = train_df.drop(columns=['quality', 'high_quality'])
        target = train_df['high_quality']
        model = RandomForestClassifier(random_state=42)
        model.fit(features, target)
        logger.info("Model trained successfully.")
        return model
    except Exception as e:
        logger.error(f"Error in model training: {e}")
        raise

def validate_model(model: RandomForestClassifier, val_df: pd.DataFrame) -> dict:
    try:
        features = val_df.drop(columns=['quality', 'high_quality'])
        target = val_df['high_quality']
        predictions = model.predict(features)
        metrics = {
            'accuracy': accuracy_score(target, predictions),
            'precision': precision_score(target, predictions),
            'recall': recall_score(target, predictions),
            'f1_score': f1_score(target, predictions)
        }
        logger.info(f"Validation metrics: {metrics}")
        return metrics
    except Exception as e:
        logger.error(f"Error in model validation: {e}")
        raise

def test_model(model: RandomForestClassifier, test_df: pd.DataFrame) -> dict:
    try:
        features = test_df.drop(columns=['quality', 'high_quality'])
        target = test_df['high_quality']
        predictions = model.predict(features)
        metrics = {
            'accuracy': accuracy_score(target, predictions),
            'precision': precision_score(target, predictions),
            'recall': recall_score(target, predictions),
            'f1_score': f1_score(target, predictions)
        }
        logger.info(f"Test metrics: {metrics}")
        return metrics
    except Exception as e:
        logger.error(f"Error in model testing: {e}")
        raise

def log_experiment(model, metrics, params=None):
    try:
        with mlflow.start_run():
            if params:
                mlflow.log_params(params)
            mlflow.log_metrics(metrics)
            mlflow.sklearn.log_model(model, "model")
            logger.info("Experiment logged successfully.")
    except Exception as e:
        logger.error(f"Error in logging experiment: {e}")
        raise

def main():
    file_path = '/dbfs/FileStore/winequality-white.csv'
    df = load_data(file_path)
    train_df, val_df, test_df = preprocess_data(df)
    model = train_model(train_df)
    val_metrics = validate_model(model, val_df)
    test_metrics = test_model(model, test_df)
    log_experiment(model, test_metrics)

if __name__ == "__main__":
    main()
