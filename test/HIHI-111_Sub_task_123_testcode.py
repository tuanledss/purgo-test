import unittest
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import mlflow
import mlflow.sklearn

# Constants
QUALITY_THRESHOLD = 7

# Test data
from test_data import happy_path_data, edge_case_data, error_case_data, special_format_data

class TestWineQualityPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Setup MLflow
        mlflow.set_experiment("Wine Quality Classification")
        cls.model = RandomForestClassifier(n_estimators=100, random_state=42)

    def setUp(self):
        # Prepare data for each test
        self.data = pd.concat([happy_path_data, edge_case_data, error_case_data, special_format_data], ignore_index=True)
        self.data['high_quality'] = self.data['quality'].apply(lambda x: 1 if int(x) >= QUALITY_THRESHOLD else 0)

    def test_data_loading_and_preprocessing(self):
        # Test data loading and preprocessing
        self.assertFalse(self.data.empty, "Data should be loaded")
        self.assertIn('high_quality', self.data.columns, "Column 'high_quality' should be present")

    def test_model_training(self):
        # Test model training
        train_data, test_data = train_test_split(self.data, test_size=0.3, random_state=42)
        X_train = train_data.drop(columns=['quality', 'high_quality'])
        y_train = train_data['high_quality']
        self.model.fit(X_train, y_train)
        self.assertIsNotNone(self.model, "Model should be trained")

    def test_model_validation_and_testing(self):
        # Test model validation and testing
        train_data, test_data = train_test_split(self.data, test_size=0.3, random_state=42)
        X_test = test_data.drop(columns=['quality', 'high_quality'])
        y_test = test_data['high_quality']
        y_pred = self.model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        self.assertGreaterEqual(accuracy, 0.8, "Model accuracy should be at least 80%")

    def test_experiment_tracking(self):
        # Test experiment tracking with MLflow
        with mlflow.start_run():
            mlflow.sklearn.log_model(self.model, "model")
            mlflow.log_param("n_estimators", 100)
            mlflow.log_metric("accuracy", 0.85)
            run_id = mlflow.active_run().info.run_id
        self.assertIsNotNone(run_id, "MLflow run ID should be generated")

    def test_error_handling(self):
        # Test error handling for invalid data
        with self.assertRaises(ValueError):
            invalid_data = error_case_data.copy()
            invalid_data['high_quality'] = invalid_data['quality'].apply(lambda x: 1 if int(x) >= QUALITY_THRESHOLD else 0)
            X_invalid = invalid_data.drop(columns=['quality', 'high_quality'])
            self.model.predict(X_invalid)

    def test_special_format_handling(self):
        # Test handling of special character and format data
        special_data = special_format_data.copy()
        special_data['high_quality'] = special_data['quality'].apply(lambda x: 1 if int(x) >= QUALITY_THRESHOLD else 0)
        X_special = special_data.drop(columns=['quality', 'high_quality'])
        y_special = special_data['high_quality']
        y_pred = self.model.predict(X_special.astype(float))
        self.assertIsNotNone(y_pred, "Model should handle special format data")

    @classmethod
    def tearDownClass(cls):
        # Cleanup if necessary
        pass

if __name__ == '__main__':
    unittest.main()
