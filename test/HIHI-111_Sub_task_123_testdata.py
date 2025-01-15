import pandas as pd
import random

# Happy Path Test Data: Valid, expected scenarios
# These records represent typical, valid data entries for the wine quality dataset.
happy_path_data = [
    {'fixed acidity': 7.0, 'volatile acidity': 0.27, 'citric acid': 0.36, 'residual sugar': 20.7, 'chlorides': 0.045,
     'free sulfur dioxide': 45.0, 'total sulfur dioxide': 170.0, 'density': 1.001, 'pH': 3.0, 'sulphates': 0.45,
     'alcohol': 8.8, 'quality': 6},
    {'fixed acidity': 6.3, 'volatile acidity': 0.3, 'citric acid': 0.34, 'residual sugar': 1.6, 'chlorides': 0.049,
     'free sulfur dioxide': 14.0, 'total sulfur dioxide': 132.0, 'density': 0.994, 'pH': 3.3, 'sulphates': 0.49,
     'alcohol': 9.5, 'quality': 6},
    # Add more happy path records as needed
]

# Edge Case Test Data: Boundary conditions
# These records test the boundaries of the data fields, such as minimum and maximum values.
edge_case_data = [
    {'fixed acidity': 3.8, 'volatile acidity': 0.08, 'citric acid': 0.0, 'residual sugar': 0.6, 'chlorides': 0.009,
     'free sulfur dioxide': 2.0, 'total sulfur dioxide': 9.0, 'density': 0.987, 'pH': 2.72, 'sulphates': 0.22,
     'alcohol': 8.0, 'quality': 3},  # Minimum values
    {'fixed acidity': 14.2, 'volatile acidity': 1.1, 'citric acid': 1.66, 'residual sugar': 65.8, 'chlorides': 0.346,
     'free sulfur dioxide': 289.0, 'total sulfur dioxide': 440.0, 'density': 1.038, 'pH': 4.01, 'sulphates': 2.0,
     'alcohol': 14.2, 'quality': 9},  # Maximum values
    # Add more edge case records as needed
]

# Error Case Test Data: Invalid inputs
# These records contain invalid data to test the system's error handling capabilities.
error_case_data = [
    {'fixed acidity': -1.0, 'volatile acidity': 0.27, 'citric acid': 0.36, 'residual sugar': 20.7, 'chlorides': 0.045,
     'free sulfur dioxide': 45.0, 'total sulfur dioxide': 170.0, 'density': 1.001, 'pH': 3.0, 'sulphates': 0.45,
     'alcohol': 8.8, 'quality': 6},  # Negative fixed acidity
    {'fixed acidity': 7.0, 'volatile acidity': 0.27, 'citric acid': 0.36, 'residual sugar': 20.7, 'chlorides': 0.045,
     'free sulfur dioxide': 45.0, 'total sulfur dioxide': 170.0, 'density': 1.001, 'pH': 3.0, 'sulphates': 0.45,
     'alcohol': 8.8, 'quality': 11},  # Invalid quality score
    # Add more error case records as needed
]

# Special Character and Format Test Data
# These records include special characters and unusual formats to test data parsing and handling.
special_character_data = [
    {'fixed acidity': 7.0, 'volatile acidity': 0.27, 'citric acid': 0.36, 'residual sugar': '20.7%', 'chlorides': 0.045,
     'free sulfur dioxide': 45.0, 'total sulfur dioxide': 170.0, 'density': 1.001, 'pH': 3.0, 'sulphates': 0.45,
     'alcohol': 8.8, 'quality': 6},  # Percentage in residual sugar
    {'fixed acidity': 7.0, 'volatile acidity': 0.27, 'citric acid': 0.36, 'residual sugar': 20.7, 'chlorides': 0.045,
     'free sulfur dioxide': 45.0, 'total sulfur dioxide': 170.0, 'density': 1.001, 'pH': '3.0*', 'sulphates': 0.45,
     'alcohol': 8.8, 'quality': 6},  # Asterisk in pH
    # Add more special character records as needed
]

# Combine all test data into a single list
test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Convert test data to DataFrame
test_df = pd.DataFrame(test_data)

# Display the test data
print(test_df)

