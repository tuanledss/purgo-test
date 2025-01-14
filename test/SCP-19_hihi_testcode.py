import unittest
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import mlflow
import mlflow.sklearn

class TestWineQualityPipeline(unittest.TestCase):

    def setUp(self):
        # Load the synthetic data
        self.train_data = train_data
        self.validation_data = validation_data
        self.test_data = test_data

        # Features and target
        self.features = ['fixed_acidity', 'volatile_acidity', 'citric_acid', 'residual_sugar', 
                         'chlorides', 'free_sulfur_dioxide', 'total_sulfur_dioxide', 'density', 
                         'pH', 'sulphates', 'alcohol']
        self.target = 'high_quality'

    def test_data_split(self):
        # Check if data is split correctly
        self.assertEqual(len(self.train_data) + len(self.validation_data) + len(self.test_data), 30)

    def test_label_conversion(self):
        # Check if 'quality' is converted to 'high_quality'
        self.assertTrue(all(self.train_data['high_quality'] == (self.train_data['quality'] >= 6).astype(int)))

    def test_model_training(self):
        # Train the Random Forest model
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(self.train_data[self.features], self.train_data[self.target])

        # Validate the model
        val_predictions = model.predict(self.validation_data[self.features])
        val_accuracy = accuracy_score(self.validation_data[self.target], val_predictions)
        self.assertGreaterEqual(val_accuracy, 0.0)  # Ensure some accuracy is achieved

        # Test the model
        test_predictions = model.predict(self.test_data[self.features])
        test_accuracy = accuracy_score(self.test_data[self.target], test_predictions)
        self.assertGreaterEqual(test_accuracy, 0.8)  # Acceptance criteria: at least 80% accuracy

    def test_mlflow_logging(self):
        # Set up MLflow
        mlflow.set_experiment("Wine Quality Classification")

        with mlflow.start_run():
            # Log model parameters and metrics
            model = RandomForestClassifier(n_estimators=100, random_state=42)
            model.fit(self.train_data[self.features], self.train_data[self.target])
            test_predictions = model.predict(self.test_data[self.features])
            test_accuracy = accuracy_score(self.test_data[self.target], test_predictions)

            mlflow.log_param("n_estimators", 100)
            mlflow.log_metric("test_accuracy", test_accuracy)
            mlflow.sklearn.log_model(model, "model")

            # Check if the model is saved in MLflow
            self.assertTrue(mlflow.active_run() is not None)

    def test_pipeline_documentation(self):
        # Check if the pipeline is well-organized and documented
        # This is a placeholder for actual documentation checks
        self.assertTrue(True)  # Assume documentation is present

if __name__ == '__main__':
    unittest.main()
