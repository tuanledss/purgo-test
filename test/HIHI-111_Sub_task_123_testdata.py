import pandas as pd
import numpy as np

# Constants for data generation
QUALITY_THRESHOLD = 7
TRAIN_SPLIT = 0.7
VALIDATION_SPLIT = 0.15
TEST_SPLIT = 0.15

# Function to generate random data for wine quality dataset
def generate_wine_data(num_records):
    np.random.seed(42)  # For reproducibility
    data = {
        'fixed acidity': np.random.uniform(4.0, 15.0, num_records),
        'volatile acidity': np.random.uniform(0.1, 1.5, num_records),
        'citric acid': np.random.uniform(0.0, 1.0, num_records),
        'residual sugar': np.random.uniform(0.5, 15.0, num_records),
        'chlorides': np.random.uniform(0.01, 0.2, num_records),
        'free sulfur dioxide': np.random.uniform(1.0, 72.0, num_records),
        'total sulfur dioxide': np.random.uniform(6.0, 289.0, num_records),
        'density': np.random.uniform(0.990, 1.003, num_records),
        'pH': np.random.uniform(2.8, 4.0, num_records),
        'sulphates': np.random.uniform(0.3, 2.0, num_records),
        'alcohol': np.random.uniform(8.0, 14.0, num_records),
        'quality': np.random.randint(3, 10, num_records)
    }
    df = pd.DataFrame(data)
    df['high_quality'] = (df['quality'] >= QUALITY_THRESHOLD).astype(int)
    return df

# Generate test data
num_records = 30
wine_data = generate_wine_data(num_records)

# Happy path test data (valid, expected scenarios)
# Test data with typical values within the expected range
happy_path_data = wine_data.iloc[:10]
print("Happy Path Test Data:")
print(happy_path_data)

# Edge case test data (boundary conditions)
# Test data with values at the edge of valid ranges
edge_case_data = pd.DataFrame({
    'fixed acidity': [4.0, 15.0],
    'volatile acidity': [0.1, 1.5],
    'citric acid': [0.0, 1.0],
    'residual sugar': [0.5, 15.0],
    'chlorides': [0.01, 0.2],
    'free sulfur dioxide': [1.0, 72.0],
    'total sulfur dioxide': [6.0, 289.0],
    'density': [0.990, 1.003],
    'pH': [2.8, 4.0],
    'sulphates': [0.3, 2.0],
    'alcohol': [8.0, 14.0],
    'quality': [3, 9]
})
edge_case_data['high_quality'] = (edge_case_data['quality'] >= QUALITY_THRESHOLD).astype(int)
print("\nEdge Case Test Data:")
print(edge_case_data)

# Error case test data (invalid inputs)
# Test data with invalid values to check error handling
error_case_data = pd.DataFrame({
    'fixed acidity': [-1.0, 20.0],  # Invalid acidity
    'volatile acidity': [-0.1, 2.0],  # Invalid volatility
    'citric acid': [-0.5, 1.5],  # Invalid citric acid
    'residual sugar': [-1.0, 20.0],  # Invalid sugar
    'chlorides': [-0.01, 0.5],  # Invalid chlorides
    'free sulfur dioxide': [-5.0, 100.0],  # Invalid sulfur dioxide
    'total sulfur dioxide': [-10.0, 300.0],  # Invalid total sulfur
    'density': [0.980, 1.010],  # Invalid density
    'pH': [2.0, 5.0],  # Invalid pH
    'sulphates': [-0.1, 3.0],  # Invalid sulphates
    'alcohol': [5.0, 20.0],  # Invalid alcohol
    'quality': [-1, 12]  # Invalid quality
})
error_case_data['high_quality'] = (error_case_data['quality'] >= QUALITY_THRESHOLD).astype(int)
print("\nError Case Test Data:")
print(error_case_data)

# Special character and format test data
# Test data with special characters and unusual formats
special_char_data = pd.DataFrame({
    'fixed acidity': ['5.0', '10.0'],
    'volatile acidity': ['0.5', '1.0'],
    'citric acid': ['0.2', '0.8'],
    'residual sugar': ['1.0', '10.0'],
    'chlorides': ['0.05', '0.15'],
    'free sulfur dioxide': ['10', '50'],
    'total sulfur dioxide': ['20', '150'],
    'density': ['0.995', '1.000'],
    'pH': ['3.0', '3.5'],
    'sulphates': ['0.5', '1.5'],
    'alcohol': ['9.0', '12.0'],
    'quality': ['5', '8']
})
special_char_data['high_quality'] = (special_char_data['quality'].astype(int) >= QUALITY_THRESHOLD).astype(int)
print("\nSpecial Character and Format Test Data:")
print(special_char_data)

