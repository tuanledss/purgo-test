import pandas as pd
import random

# Test Data Generation for Wine Quality Dataset

# Happy Path Test Data (Valid, Expected Scenarios)
# These records represent typical, valid entries in the dataset.
happy_path_data = [
    # Valid record with average values
    {"fixed acidity": 7.0, "volatile acidity": 0.27, "citric acid": 0.36, "residual sugar": 20.7, "chlorides": 0.045,
     "free sulfur dioxide": 45.0, "total sulfur dioxide": 170.0, "density": 1.001, "pH": 3.0, "sulphates": 0.45,
     "alcohol": 8.8, "quality": 6},
    # Valid record with high quality
    {"fixed acidity": 6.3, "volatile acidity": 0.3, "citric acid": 0.34, "residual sugar": 1.6, "chlorides": 0.049,
     "free sulfur dioxide": 14.0, "total sulfur dioxide": 132.0, "density": 0.994, "pH": 3.3, "sulphates": 0.49,
     "alcohol": 9.5, "quality": 8},
    # Valid record with low quality
    {"fixed acidity": 8.1, "volatile acidity": 0.28, "citric acid": 0.4, "residual sugar": 6.9, "chlorides": 0.05,
     "free sulfur dioxide": 30.0, "total sulfur dioxide": 97.0, "density": 0.9951, "pH": 3.26, "sulphates": 0.44,
     "alcohol": 10.1, "quality": 5},
]

# Edge Case Test Data (Boundary Conditions)
# These records test the boundaries of the dataset's valid range.
edge_case_data = [
    # Minimum values for each feature
    {"fixed acidity": 3.8, "volatile acidity": 0.08, "citric acid": 0.0, "residual sugar": 0.6, "chlorides": 0.009,
     "free sulfur dioxide": 1.0, "total sulfur dioxide": 6.0, "density": 0.987, "pH": 2.72, "sulphates": 0.22,
     "alcohol": 8.0, "quality": 3},
    # Maximum values for each feature
    {"fixed acidity": 14.2, "volatile acidity": 1.1, "citric acid": 1.66, "residual sugar": 65.8, "chlorides": 0.346,
     "free sulfur dioxide": 289.0, "total sulfur dioxide": 440.0, "density": 1.038, "pH": 3.82, "sulphates": 2.0,
     "alcohol": 14.2, "quality": 9},
]

# Error Case Test Data (Invalid Inputs)
# These records contain invalid data to test error handling.
error_case_data = [
    # Negative values for features
    {"fixed acidity": -1.0, "volatile acidity": -0.5, "citric acid": -0.1, "residual sugar": -2.0, "chlorides": -0.01,
     "free sulfur dioxide": -5.0, "total sulfur dioxide": -10.0, "density": -0.99, "pH": -3.0, "sulphates": -0.5,
     "alcohol": -9.0, "quality": -1},
    # Non-numeric values for features
    {"fixed acidity": "NaN", "volatile acidity": "NaN", "citric acid": "NaN", "residual sugar": "NaN", "chlorides": "NaN",
     "free sulfur dioxide": "NaN", "total sulfur dioxide": "NaN", "density": "NaN", "pH": "NaN", "sulphates": "NaN",
     "alcohol": "NaN", "quality": "NaN"},
]

# Special Character and Format Test Data
# These records include special characters and unusual formats.
special_character_data = [
    # Special characters in numeric fields
    {"fixed acidity": "7.0$", "volatile acidity": "0.27%", "citric acid": "0.36#", "residual sugar": "20.7@", "chlorides": "0.045&",
     "free sulfur dioxide": "45.0*", "total sulfur dioxide": "170.0^", "density": "1.001!", "pH": "3.0(", "sulphates": "0.45)",
     "alcohol": "8.8_", "quality": "6+"},
    # Mixed data types
    {"fixed acidity": 7.0, "volatile acidity": "0.27", "citric acid": 0.36, "residual sugar": "20.7", "chlorides": 0.045,
     "free sulfur dioxide": "45", "total sulfur dioxide": 170.0, "density": "1.001", "pH": 3.0, "sulphates": "0.45",
     "alcohol": 8.8, "quality": "6"},
]

# Combine all test data into a single list
test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Convert test data to DataFrame
test_df = pd.DataFrame(test_data)

# Display the generated test data
print(test_df)

