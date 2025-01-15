import pandas as pd
import numpy as np

# Happy Path Test Data: Valid, expected scenarios
# These records represent typical, valid data entries for the wine quality dataset.
happy_path_data = pd.DataFrame({
    'fixed acidity': [7.0, 6.3, 8.1, 7.2, 6.8],
    'volatile acidity': [0.27, 0.30, 0.28, 0.23, 0.26],
    'citric acid': [0.36, 0.34, 0.40, 0.32, 0.35],
    'residual sugar': [20.7, 1.6, 6.9, 8.5, 7.0],
    'chlorides': [0.045, 0.049, 0.050, 0.044, 0.046],
    'free sulfur dioxide': [45.0, 14.0, 30.0, 47.0, 35.0],
    'total sulfur dioxide': [170.0, 132.0, 97.0, 186.0, 104.0],
    'density': [1.001, 0.994, 0.995, 0.996, 0.994],
    'pH': [3.0, 3.3, 3.2, 3.1, 3.2],
    'sulphates': [0.45, 0.49, 0.44, 0.42, 0.47],
    'alcohol': [8.8, 9.5, 10.1, 9.9, 10.0],
    'quality': [6, 6, 6, 6, 6]
})

# Edge Case Test Data: Boundary conditions
# These records test the boundaries of the data, such as minimum and maximum values.
edge_case_data = pd.DataFrame({
    'fixed acidity': [4.6, 15.9],  # Min and max fixed acidity
    'volatile acidity': [0.12, 1.58],  # Min and max volatile acidity
    'citric acid': [0.0, 1.66],  # Min and max citric acid
    'residual sugar': [0.6, 65.8],  # Min and max residual sugar
    'chlorides': [0.009, 0.346],  # Min and max chlorides
    'free sulfur dioxide': [2.0, 289.0],  # Min and max free sulfur dioxide
    'total sulfur dioxide': [9.0, 440.0],  # Min and max total sulfur dioxide
    'density': [0.987, 1.038],  # Min and max density
    'pH': [2.72, 3.82],  # Min and max pH
    'sulphates': [0.22, 2.0],  # Min and max sulphates
    'alcohol': [8.0, 14.2],  # Min and max alcohol
    'quality': [3, 9]  # Min and max quality
})

# Error Case Test Data: Invalid inputs
# These records contain invalid data to test the system's error handling.
error_case_data = pd.DataFrame({
    'fixed acidity': [-1.0, 20.0],  # Invalid fixed acidity
    'volatile acidity': [0.5, -0.1],  # Invalid volatile acidity
    'citric acid': [0.5, -0.2],  # Invalid citric acid
    'residual sugar': [-5.0, 70.0],  # Invalid residual sugar
    'chlorides': [0.5, -0.1],  # Invalid chlorides
    'free sulfur dioxide': [-10.0, 300.0],  # Invalid free sulfur dioxide
    'total sulfur dioxide': [500.0, -20.0],  # Invalid total sulfur dioxide
    'density': [0.5, 2.0],  # Invalid density
    'pH': [1.0, 5.0],  # Invalid pH
    'sulphates': [-0.5, 3.0],  # Invalid sulphates
    'alcohol': [0.0, 20.0],  # Invalid alcohol
    'quality': [-1, 15]  # Invalid quality
})

# Special Character and Format Test Data
# These records include special characters and unusual formats to test data parsing.
special_character_data = pd.DataFrame({
    'fixed acidity': ['7.0', '6.3'],  # String format
    'volatile acidity': ['0.27', '0.30'],  # String format
    'citric acid': ['0.36', '0.34'],  # String format
    'residual sugar': ['20.7', '1.6'],  # String format
    'chlorides': ['0.045', '0.049'],  # String format
    'free sulfur dioxide': ['45.0', '14.0'],  # String format
    'total sulfur dioxide': ['170.0', '132.0'],  # String format
    'density': ['1.001', '0.994'],  # String format
    'pH': ['3.0', '3.3'],  # String format
    'sulphates': ['0.45', '0.49'],  # String format
    'alcohol': ['8.8', '9.5'],  # String format
    'quality': ['6', '6']  # String format
})

# Combine all test data into a single DataFrame
test_data = pd.concat([happy_path_data, edge_case_data, error_case_data, special_character_data], ignore_index=True)

# Display the test data
print(test_data)

