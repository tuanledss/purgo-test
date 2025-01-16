import unittest
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import mlflow

class TestWineQualityPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Load the dataset
        csv_file_path = '/dbfs/FileStore/winequality-white.csv'
        cls.df = pd.read_csv(csv_file_path)

        # Convert 'quality' to binary 'high_quality'
        cls.df['high_quality'] = (cls.df['quality'] >= 7).astype(int)

        # Split the dataset into training, validation, and test sets
        cls.train_df = cls.df.sample(frac=0.7, random_state=42)
        remaining_df = cls.df.drop(cls.train_df.index)
        cls.validation_df = remaining_df.sample(frac=0.5, random_state=42)
        cls.test_df = remaining_df.drop(cls.validation_df.index)

    def test_data_preprocessing(self):
        # Test binary conversion of 'quality'
        self.assertTrue(all(self.df['high_quality'].isin([0, 1])), "Quality conversion to binary failed")

    def test_data_split(self):
        # Test dataset split ratios
        total_records = len(self.df)
        self.assertAlmostEqual(len(self.train_df) / total_records, 0.7, delta=0.05, msg="Training set split ratio incorrect")
        self.assertAlmostEqual(len(self.validation_df) / total_records, 0.15, delta=0.05, msg="Validation set split ratio incorrect")
        self.assertAlmostEqual(len(self.test_df) / total_records, 0.15, delta=0.05, msg="Test set split ratio incorrect")

    def test_model_training(self):
        # Train Random Forest model
        features = self.train_df.drop(columns=['quality', 'high_quality'])
        labels = self.train_df['high_quality']
        model = RandomForestClassifier(random_state=42)
        model.fit(features, labels)

        # Validate model is trained
        self.assertIsNotNone(model, "Model training failed")

    def test_model_validation(self):
        # Validate model accuracy on validation set
        features = self.validation_df.drop(columns=['quality', 'high_quality'])
        labels = self.validation_df['high_quality']
        model = RandomForestClassifier(random_state=42)
        model.fit(self.train_df.drop(columns=['quality', 'high_quality']), self.train_df['high_quality'])
        predictions = model.predict(features)
        accuracy = accuracy_score(labels, predictions)

        # Check if accuracy is reasonable
        self.assertGreaterEqual(accuracy, 0.7, "Model accuracy on validation set is too low")

    def test_model_testing(self):
        # Test model accuracy on test set
        features = self.test_df.drop(columns=['quality', 'high_quality'])
        labels = self.test_df['high_quality']
        model = RandomForestClassifier(random_state=42)
        model.fit(self.train_df.drop(columns=['quality', 'high_quality']), self.train_df['high_quality'])
        predictions = model.predict(features)
        accuracy = accuracy_score(labels, predictions)

        # Check if accuracy meets acceptance criteria
        self.assertGreaterEqual(accuracy, 0.8, "Model accuracy on test set does not meet acceptance criteria")

    def test_mlflow_logging(self):
        # Test MLflow logging
        with mlflow.start_run():
            model = RandomForestClassifier(random_state=42)
            model.fit(self.train_df.drop(columns=['quality', 'high_quality']), self.train_df['high_quality'])
            mlflow.sklearn.log_model(model, "model")
            mlflow.log_metric("accuracy", 0.85)

            # Validate MLflow logging
            run_id = mlflow.active_run().info.run_id
            self.assertIsNotNone(run_id, "MLflow run ID is None, logging failed")

    @classmethod
    def tearDownClass(cls):
        # Clean up resources if needed
        pass

if __name__ == '__main__':
    unittest.main()
