import pandas as pd
import random
import string

# Function to generate random string with special characters
def random_string_with_special_chars(length=10):
    chars = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(chars) for _ in range(length))

# Happy path test data (valid, expected scenarios)
# These records are expected to pass all validations and represent typical data entries.
happy_path_data = [
    {'fixed acidity': 7.0, 'volatile acidity': 0.27, 'citric acid': 0.36, 'residual sugar': 20.7, 'chlorides': 0.045,
     'free sulfur dioxide': 45.0, 'total sulfur dioxide': 170.0, 'density': 1.001, 'pH': 3.0, 'sulphates': 0.45,
     'alcohol': 8.8, 'quality': 6},
    {'fixed acidity': 6.3, 'volatile acidity': 0.3, 'citric acid': 0.34, 'residual sugar': 1.6, 'chlorides': 0.049,
     'free sulfur dioxide': 14.0, 'total sulfur dioxide': 132.0, 'density': 0.994, 'pH': 3.3, 'sulphates': 0.49,
     'alcohol': 9.5, 'quality': 6},
    # Add more happy path records as needed
]

# Edge case test data (boundary conditions)
# These records test the boundaries of valid input ranges.
edge_case_data = [
    {'fixed acidity': 4.0, 'volatile acidity': 0.12, 'citric acid': 0.0, 'residual sugar': 0.6, 'chlorides': 0.009,
     'free sulfur dioxide': 1.0, 'total sulfur dioxide': 6.0, 'density': 0.987, 'pH': 2.72, 'sulphates': 0.22,
     'alcohol': 8.0, 'quality': 3},  # Minimum values
    {'fixed acidity': 15.9, 'volatile acidity': 1.58, 'citric acid': 1.66, 'residual sugar': 65.8, 'chlorides': 0.611,
     'free sulfur dioxide': 289.0, 'total sulfur dioxide': 440.0, 'density': 1.038, 'pH': 3.82, 'sulphates': 2.0,
     'alcohol': 14.2, 'quality': 9},  # Maximum values
    # Add more edge case records as needed
]

# Error case test data (invalid inputs)
# These records are expected to fail validation checks.
error_case_data = [
    {'fixed acidity': -1.0, 'volatile acidity': 0.27, 'citric acid': 0.36, 'residual sugar': 20.7, 'chlorides': 0.045,
     'free sulfur dioxide': 45.0, 'total sulfur dioxide': 170.0, 'density': 1.001, 'pH': 3.0, 'sulphates': 0.45,
     'alcohol': 8.8, 'quality': 6},  # Negative fixed acidity
    {'fixed acidity': 7.0, 'volatile acidity': 0.27, 'citric acid': 0.36, 'residual sugar': 20.7, 'chlorides': 0.045,
     'free sulfur dioxide': 45.0, 'total sulfur dioxide': 170.0, 'density': 1.001, 'pH': 3.0, 'sulphates': 0.45,
     'alcohol': 8.8, 'quality': 11},  # Quality out of range
    # Add more error case records as needed
]

# Special character and format test data
# These records test the handling of special characters and unusual formats.
special_char_data = [
    {'fixed acidity': 7.0, 'volatile acidity': 0.27, 'citric acid': 0.36, 'residual sugar': 20.7, 'chlorides': 0.045,
     'free sulfur dioxide': 45.0, 'total sulfur dioxide': 170.0, 'density': 1.001, 'pH': 3.0, 'sulphates': 0.45,
     'alcohol': 8.8, 'quality': random_string_with_special_chars()},  # Quality with special characters
    {'fixed acidity': '7.0', 'volatile acidity': '0.27', 'citric acid': '0.36', 'residual sugar': '20.7',
     'chlorides': '0.045', 'free sulfur dioxide': '45.0', 'total sulfur dioxide': '170.0', 'density': '1.001',
     'pH': '3.0', 'sulphates': '0.45', 'alcohol': '8.8', 'quality': '6'},  # All fields as strings
    # Add more special character records as needed
]

# Combine all test data into a single list
test_data = happy_path_data + edge_case_data + error_case_data + special_char_data

# Convert test data to DataFrame for further processing
test_df = pd.DataFrame(test_data)

# Display the generated test data
print(test_df)

