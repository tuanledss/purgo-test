import pandas as pd
import numpy as np

# Constants for data generation
QUALITY_THRESHOLD = 7  # Threshold to classify high quality
NUM_RECORDS = 30  # Total number of records to generate

# Helper function to generate random data within a range
def generate_random_data(min_val, max_val, size):
    return np.random.uniform(min_val, max_val, size)

# Happy path test data (valid, expected scenarios)
# These records are expected to pass all validations and represent typical data
happy_path_data = pd.DataFrame({
    'fixed acidity': generate_random_data(5.0, 9.0, 10),
    'volatile acidity': generate_random_data(0.1, 0.5, 10),
    'citric acid': generate_random_data(0.0, 0.5, 10),
    'residual sugar': generate_random_data(1.0, 10.0, 10),
    'chlorides': generate_random_data(0.01, 0.1, 10),
    'free sulfur dioxide': generate_random_data(10.0, 50.0, 10),
    'total sulfur dioxide': generate_random_data(50.0, 150.0, 10),
    'density': generate_random_data(0.990, 1.000, 10),
    'pH': generate_random_data(3.0, 3.5, 10),
    'sulphates': generate_random_data(0.3, 0.7, 10),
    'alcohol': generate_random_data(9.0, 12.0, 10),
    'quality': np.random.randint(3, 9, 10)
})
happy_path_data['high_quality'] = happy_path_data['quality'] >= QUALITY_THRESHOLD

# Edge case test data (boundary conditions)
# These records test the boundaries of valid input ranges
edge_case_data = pd.DataFrame({
    'fixed acidity': [5.0, 9.0],
    'volatile acidity': [0.1, 0.5],
    'citric acid': [0.0, 0.5],
    'residual sugar': [1.0, 10.0],
    'chlorides': [0.01, 0.1],
    'free sulfur dioxide': [10.0, 50.0],
    'total sulfur dioxide': [50.0, 150.0],
    'density': [0.990, 1.000],
    'pH': [3.0, 3.5],
    'sulphates': [0.3, 0.7],
    'alcohol': [9.0, 12.0],
    'quality': [3, 8]
})
edge_case_data['high_quality'] = edge_case_data['quality'] >= QUALITY_THRESHOLD

# Error case test data (invalid inputs)
# These records contain invalid data to test error handling
error_case_data = pd.DataFrame({
    'fixed acidity': [-1.0, 15.0],  # Invalid acidity values
    'volatile acidity': [0.6, -0.1],  # Invalid volatile acidity
    'citric acid': [0.6, -0.1],  # Invalid citric acid
    'residual sugar': [-5.0, 20.0],  # Invalid residual sugar
    'chlorides': [0.2, -0.05],  # Invalid chlorides
    'free sulfur dioxide': [-10.0, 100.0],  # Invalid free sulfur dioxide
    'total sulfur dioxide': [200.0, -50.0],  # Invalid total sulfur dioxide
    'density': [1.1, 0.98],  # Invalid density
    'pH': [2.5, 4.0],  # Invalid pH
    'sulphates': [0.8, -0.2],  # Invalid sulphates
    'alcohol': [5.0, 15.0],  # Invalid alcohol
    'quality': [-1, 11]  # Invalid quality
})
error_case_data['high_quality'] = error_case_data['quality'] >= QUALITY_THRESHOLD

# Special character and format test data
# These records test the handling of special characters and formats
special_char_data = pd.DataFrame({
    'fixed acidity': ['5.0', '9.0'],  # String format
    'volatile acidity': ['0.1', '0.5'],  # String format
    'citric acid': ['0.0', '0.5'],  # String format
    'residual sugar': ['1.0', '10.0'],  # String format
    'chlorides': ['0.01', '0.1'],  # String format
    'free sulfur dioxide': ['10.0', '50.0'],  # String format
    'total sulfur dioxide': ['50.0', '150.0'],  # String format
    'density': ['0.990', '1.000'],  # String format
    'pH': ['3.0', '3.5'],  # String format
    'sulphates': ['0.3', '0.7'],  # String format
    'alcohol': ['9.0', '12.0'],  # String format
    'quality': ['3', '8']  # String format
})
special_char_data['high_quality'] = special_char_data['quality'].astype(int) >= QUALITY_THRESHOLD

# Combine all test data into a single DataFrame
test_data = pd.concat([happy_path_data, edge_case_data, error_case_data, special_char_data], ignore_index=True)

# Display the generated test data
print(test_data)
