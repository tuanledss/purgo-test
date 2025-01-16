import unittest
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import mlflow
import mlflow.sklearn

# Mock function to simulate data loading
def load_data():
    # This function would normally load data from a CSV or database
    return pd.DataFrame([
        {'fixed acidity': 7.0, 'volatile acidity': 0.27, 'citric acid': 0.36, 'residual sugar': 20.7, 'chlorides': 0.045,
         'free sulfur dioxide': 45.0, 'total sulfur dioxide': 170.0, 'density': 1.001, 'pH': 3.0, 'sulphates': 0.45,
         'alcohol': 8.8, 'quality': 6},
        {'fixed acidity': 6.3, 'volatile acidity': 0.3, 'citric acid': 0.34, 'residual sugar': 1.6, 'chlorides': 0.049,
         'free sulfur dioxide': 14.0, 'total sulfur dioxide': 132.0, 'density': 0.994, 'pH': 3.3, 'sulphates': 0.49,
         'alcohol': 9.5, 'quality': 6},
        # Add more records as needed
    ])

# Function to preprocess data
def preprocess_data(df):
    df['high_quality'] = df['quality'].apply(lambda x: 1 if x >= 7 else 0)
    return df.drop(columns=['quality'])

# Function to train the model
def train_model(X_train, y_train):
    model = RandomForestClassifier()
    model.fit(X_train, y_train)
    return model

# Function to evaluate the model
def evaluate_model(model, X_test, y_test):
    predictions = model.predict(X_test)
    return accuracy_score(y_test, predictions)

# Test class for the wine quality classification pipeline
class TestWineQualityPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Load and preprocess data
        cls.data = load_data()
        cls.data = preprocess_data(cls.data)
        cls.X = cls.data.drop(columns=['high_quality'])
        cls.y = cls.data['high_quality']
        cls.X_train, cls.X_test, cls.y_train, cls.y_test = train_test_split(cls.X, cls.y, test_size=0.3, random_state=42)

    def test_data_loading(self):
        # Test if data is loaded correctly
        self.assertIsInstance(self.data, pd.DataFrame)
        self.assertFalse(self.data.empty)

    def test_data_preprocessing(self):
        # Test if data is preprocessed correctly
        self.assertIn('high_quality', self.data.columns)
        self.assertNotIn('quality', self.data.columns)

    def test_model_training(self):
        # Test if model is trained without errors
        model = train_model(self.X_train, self.y_train)
        self.assertIsInstance(model, RandomForestClassifier)

    def test_model_accuracy(self):
        # Test if model achieves at least 80% accuracy
        model = train_model(self.X_train, self.y_train)
        accuracy = evaluate_model(model, self.X_test, self.y_test)
        self.assertGreaterEqual(accuracy, 0.8)

    def test_mlflow_logging(self):
        # Test if MLflow logs are created
        with mlflow.start_run():
            model = train_model(self.X_train, self.y_train)
            accuracy = evaluate_model(model, self.X_test, self.y_test)
            mlflow.log_param("model_type", "RandomForest")
            mlflow.log_metric("accuracy", accuracy)
            mlflow.sklearn.log_model(model, "model")
            self.assertTrue(mlflow.active_run() is not None)

    def test_error_handling(self):
        # Test error handling for invalid data
        with self.assertRaises(ValueError):
            invalid_data = pd.DataFrame([{'fixed acidity': -1.0}])
            preprocess_data(invalid_data)

    @classmethod
    def tearDownClass(cls):
        # Clean up resources if needed
        pass

if __name__ == '__main__':
    unittest.main()
