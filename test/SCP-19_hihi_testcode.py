import unittest
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import mlflow
import mlflow.sklearn

class TestWineQualityPipeline(unittest.TestCase):

    def setUp(self):
        # Load the synthetic data
        self.data = data
        self.train_data = train_data
        self.validation_data = validation_data
        self.test_data = test_data

    def test_data_splitting(self):
        # Validate data splitting
        self.assertEqual(len(self.train_data) + len(self.validation_data) + len(self.test_data), len(self.data))
        self.assertTrue(set(self.train_data.index).isdisjoint(set(self.validation_data.index)))
        self.assertTrue(set(self.train_data.index).isdisjoint(set(self.test_data.index)))
        self.assertTrue(set(self.validation_data.index).isdisjoint(set(self.test_data.index)))

    def test_label_conversion(self):
        # Validate binary conversion of 'quality' to 'high_quality'
        self.assertTrue(all(self.data['high_quality'] == (self.data['quality'] >= 6).astype(int)))

    def test_model_training_and_accuracy(self):
        # Train Random Forest model
        X_train = self.train_data.drop(columns=['quality', 'high_quality'])
        y_train = self.train_data['high_quality']
        model = RandomForestClassifier(random_state=42)
        model.fit(X_train, y_train)

        # Validate model on validation data
        X_val = self.validation_data.drop(columns=['quality', 'high_quality'])
        y_val = self.validation_data['high_quality']
        val_predictions = model.predict(X_val)
        val_accuracy = accuracy_score(y_val, val_predictions)
        print(f"Validation Accuracy: {val_accuracy}")

        # Test model on test data
        X_test = self.test_data.drop(columns=['quality', 'high_quality'])
        y_test = self.test_data['high_quality']
        test_predictions = model.predict(X_test)
        test_accuracy = accuracy_score(y_test, test_predictions)
        print(f"Test Accuracy: {test_accuracy}")

        # Validate test accuracy
        self.assertGreaterEqual(test_accuracy, 0.80)

    def test_mlflow_logging(self):
        # Set up MLflow
        mlflow.set_experiment("Wine Quality Classification")
        with mlflow.start_run():
            # Log model parameters and metrics
            model = RandomForestClassifier(random_state=42)
            model.fit(self.train_data.drop(columns=['quality', 'high_quality']), self.train_data['high_quality'])
            mlflow.sklearn.log_model(model, "model")
            mlflow.log_metric("test_accuracy", accuracy_score(self.test_data['high_quality'], model.predict(self.test_data.drop(columns=['quality', 'high_quality']))))

            # Validate MLflow logging
            run_id = mlflow.active_run().info.run_id
            self.assertIsNotNone(run_id)

if __name__ == '__main__':
    unittest.main()
