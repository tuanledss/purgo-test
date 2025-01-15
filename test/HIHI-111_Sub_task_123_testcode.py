import unittest
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import mlflow
import mlflow.sklearn

# Constants
QUALITY_THRESHOLD = 7
ACCURACY_THRESHOLD = 0.80

# Mock data for testing
from test_data import happy_path_data, edge_case_data, error_case_data, special_char_data

class TestWineQualityPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Setup MLflow
        mlflow.set_experiment("Wine Quality Classification")
        cls.model = RandomForestClassifier()

    def setUp(self):
        # Setup for each test
        self.data = happy_path_data.copy()

    def test_data_loading(self):
        # Test if data is loaded correctly
        self.assertIsInstance(self.data, pd.DataFrame)
        self.assertFalse(self.data.empty)

    def test_data_preprocessing(self):
        # Test if 'quality' is converted to 'high_quality'
        self.data['high_quality'] = self.data['quality'] >= QUALITY_THRESHOLD
        self.assertIn('high_quality', self.data.columns)
        self.assertTrue(self.data['high_quality'].isin([True, False]).all())

    def test_data_splitting(self):
        # Test if data is split into train, validation, and test sets
        train, test = train_test_split(self.data, test_size=0.3, random_state=42)
        self.assertGreater(len(train), 0)
        self.assertGreater(len(test), 0)

    def test_model_training(self):
        # Test if model trains without errors
        train, test = train_test_split(self.data, test_size=0.3, random_state=42)
        X_train = train.drop(['quality', 'high_quality'], axis=1)
        y_train = train['high_quality']
        self.model.fit(X_train, y_train)
        self.assertIsNotNone(self.model)

    def test_model_accuracy(self):
        # Test if model achieves required accuracy
        train, test = train_test_split(self.data, test_size=0.3, random_state=42)
        X_test = test.drop(['quality', 'high_quality'], axis=1)
        y_test = test['high_quality']
        predictions = self.model.predict(X_test)
        accuracy = accuracy_score(y_test, predictions)
        self.assertGreaterEqual(accuracy, ACCURACY_THRESHOLD)

    def test_experiment_tracking(self):
        # Test if experiments are logged in MLflow
        with mlflow.start_run():
            mlflow.sklearn.log_model(self.model, "model")
            mlflow.log_metric("accuracy", 0.85)
            run_id = mlflow.active_run().info.run_id
        self.assertIsNotNone(run_id)

    def test_edge_cases(self):
        # Test edge cases
        self.data = edge_case_data.copy()
        self.test_model_accuracy()

    def test_error_handling(self):
        # Test error scenarios
        self.data = error_case_data.copy()
        with self.assertRaises(ValueError):
            self.test_model_training()

    def test_special_character_handling(self):
        # Test special character handling
        self.data = special_char_data.copy()
        self.data = self.data.apply(pd.to_numeric, errors='coerce')
        self.assertFalse(self.data.isnull().values.any())

    @classmethod
    def tearDownClass(cls):
        # Teardown MLflow
        mlflow.end_run()

if __name__ == '__main__':
    unittest.main()
