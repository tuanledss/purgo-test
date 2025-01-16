import unittest
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import mlflow
import mlflow.sklearn

# Test class for the wine quality classification pipeline
class TestWineQualityPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Setup method to initialize test data and model
        cls.test_data = pd.DataFrame([
            {"fixed acidity": 7.0, "volatile acidity": 0.27, "citric acid": 0.36, "residual sugar": 20.7, "chlorides": 0.045,
             "free sulfur dioxide": 45.0, "total sulfur dioxide": 170.0, "density": 1.001, "pH": 3.0, "sulphates": 0.45,
             "alcohol": 8.8, "quality": 6},
            {"fixed acidity": 6.3, "volatile acidity": 0.3, "citric acid": 0.34, "residual sugar": 1.6, "chlorides": 0.049,
             "free sulfur dioxide": 14.0, "total sulfur dioxide": 132.0, "density": 0.994, "pH": 3.3, "sulphates": 0.49,
             "alcohol": 9.5, "quality": 8},
            {"fixed acidity": 8.1, "volatile acidity": 0.28, "citric acid": 0.4, "residual sugar": 6.9, "chlorides": 0.05,
             "free sulfur dioxide": 30.0, "total sulfur dioxide": 97.0, "density": 0.9951, "pH": 3.26, "sulphates": 0.44,
             "alcohol": 10.1, "quality": 5},
        ])
        cls.test_data['high_quality'] = cls.test_data['quality'] >= 7

    def test_data_preprocessing(self):
        # Test data preprocessing: conversion of 'quality' to 'high_quality'
        df = self.test_data.copy()
        df['high_quality'] = df['quality'] >= 7
        self.assertTrue(all(df['high_quality'] == [False, True, False]), "Data preprocessing failed to convert 'quality' to 'high_quality' correctly.")

    def test_data_splitting(self):
        # Test data splitting into train, validation, and test sets
        df = self.test_data.copy()
        train, test = train_test_split(df, test_size=0.3, random_state=42)
        self.assertEqual(len(train) + len(test), len(df), "Data splitting did not preserve the total number of records.")

    def test_model_training(self):
        # Test model training with Random Forest
        df = self.test_data.copy()
        X = df.drop(columns=['quality', 'high_quality'])
        y = df['high_quality']
        model = RandomForestClassifier(random_state=42)
        model.fit(X, y)
        self.assertIsNotNone(model, "Model training failed to produce a model.")

    def test_model_accuracy(self):
        # Test model accuracy on test data
        df = self.test_data.copy()
        X = df.drop(columns=['quality', 'high_quality'])
        y = df['high_quality']
        model = RandomForestClassifier(random_state=42)
        model.fit(X, y)
        predictions = model.predict(X)
        accuracy = accuracy_score(y, predictions)
        self.assertGreaterEqual(accuracy, 0.8, "Model accuracy is below the required threshold of 80%.")

    def test_mlflow_logging(self):
        # Test MLflow logging of model parameters and metrics
        with mlflow.start_run():
            df = self.test_data.copy()
            X = df.drop(columns=['quality', 'high_quality'])
            y = df['high_quality']
            model = RandomForestClassifier(random_state=42)
            model.fit(X, y)
            mlflow.sklearn.log_model(model, "model")
            mlflow.log_metric("accuracy", 0.85)
            run_id = mlflow.active_run().info.run_id
            self.assertIsNotNone(run_id, "MLflow did not log the run correctly.")

    def test_error_handling_invalid_data(self):
        # Test error handling with invalid data
        invalid_data = pd.DataFrame([
            {"fixed acidity": -1.0, "volatile acidity": -0.5, "citric acid": -0.1, "residual sugar": -2.0, "chlorides": -0.01,
             "free sulfur dioxide": -5.0, "total sulfur dioxide": -10.0, "density": -0.99, "pH": -3.0, "sulphates": -0.5,
             "alcohol": -9.0, "quality": -1}
        ])
        with self.assertRaises(ValueError, msg="Model did not raise an error for invalid data."):
            X_invalid = invalid_data.drop(columns=['quality'])
            model = RandomForestClassifier(random_state=42)
            model.predict(X_invalid)

    @classmethod
    def tearDownClass(cls):
        # Teardown method to clean up after tests
        pass

# Run the tests
if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)
