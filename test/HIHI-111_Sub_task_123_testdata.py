import pandas as pd
import numpy as np

# Constants for data generation
QUALITY_THRESHOLD = 7  # Threshold for high quality wine
NUM_RECORDS = 30  # Total number of records to generate

# Helper function to generate random data within a range
def generate_random_data(min_val, max_val, size):
    return np.random.uniform(min_val, max_val, size)

# Happy path test data (valid, expected scenarios)
# These records are expected to pass all validations and represent typical data
happy_path_data = pd.DataFrame({
    'fixed acidity': generate_random_data(5.0, 9.0, 10),
    'volatile acidity': generate_random_data(0.1, 0.5, 10),
    'citric acid': generate_random_data(0.0, 0.4, 10),
    'residual sugar': generate_random_data(1.0, 6.0, 10),
    'chlorides': generate_random_data(0.01, 0.1, 10),
    'free sulfur dioxide': generate_random_data(10.0, 40.0, 10),
    'total sulfur dioxide': generate_random_data(50.0, 150.0, 10),
    'density': generate_random_data(0.990, 1.000, 10),
    'pH': generate_random_data(3.0, 3.5, 10),
    'sulphates': generate_random_data(0.4, 0.7, 10),
    'alcohol': generate_random_data(9.0, 12.0, 10),
    'quality': np.random.randint(5, 9, 10)  # Random quality between 5 and 8
})

# Edge case test data (boundary conditions)
# These records test the boundaries of valid input ranges
edge_case_data = pd.DataFrame({
    'fixed acidity': [5.0, 9.0],  # Min and max boundary
    'volatile acidity': [0.1, 0.5],
    'citric acid': [0.0, 0.4],
    'residual sugar': [1.0, 6.0],
    'chlorides': [0.01, 0.1],
    'free sulfur dioxide': [10.0, 40.0],
    'total sulfur dioxide': [50.0, 150.0],
    'density': [0.990, 1.000],
    'pH': [3.0, 3.5],
    'sulphates': [0.4, 0.7],
    'alcohol': [9.0, 12.0],
    'quality': [5, 8]  # Boundary quality values
})

# Error case test data (invalid inputs)
# These records contain invalid data to test error handling
error_case_data = pd.DataFrame({
    'fixed acidity': [-1.0, 15.0],  # Invalid negative and too high value
    'volatile acidity': [-0.1, 1.0],
    'citric acid': [-0.1, 1.0],
    'residual sugar': [-1.0, 20.0],
    'chlorides': [-0.01, 0.5],
    'free sulfur dioxide': [-5.0, 100.0],
    'total sulfur dioxide': [-10.0, 300.0],
    'density': [0.980, 1.050],
    'pH': [2.0, 4.0],
    'sulphates': [-0.1, 1.5],
    'alcohol': [5.0, 15.0],
    'quality': [-1, 10]  # Invalid quality values
})

# Special character and format test data
# These records include special characters and unusual formats
special_format_data = pd.DataFrame({
    'fixed acidity': ['5.0', '9.0'],  # String format
    'volatile acidity': ['0.1', '0.5'],
    'citric acid': ['0.0', '0.4'],
    'residual sugar': ['1.0', '6.0'],
    'chlorides': ['0.01', '0.1'],
    'free sulfur dioxide': ['10.0', '40.0'],
    'total sulfur dioxide': ['50.0', '150.0'],
    'density': ['0.990', '1.000'],
    'pH': ['3.0', '3.5'],
    'sulphates': ['0.4', '0.7'],
    'alcohol': ['9.0', '12.0'],
    'quality': ['5', '8']  # String format
})

# Combine all test data into a single DataFrame
test_data = pd.concat([happy_path_data, edge_case_data, error_case_data, special_format_data], ignore_index=True)

# Convert 'quality' to 'high_quality' based on the threshold
test_data['high_quality'] = test_data['quality'].apply(lambda x: 1 if int(x) >= QUALITY_THRESHOLD else 0)

# Display the generated test data
print(test_data)
