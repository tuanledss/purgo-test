import pandas as pd
import random

# Happy path test data (valid, expected scenarios)
# These records represent typical, valid inputs for the wine quality dataset.
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
     "alcohol": 10.1, "quality": 5}
]

# Edge case test data (boundary conditions)
# These records test the boundaries of valid input ranges.
edge_case_data = [
    # Minimum values for each feature
    {"fixed acidity": 0.0, "volatile acidity": 0.0, "citric acid": 0.0, "residual sugar": 0.0, "chlorides": 0.0,
     "free sulfur dioxide": 0.0, "total sulfur dioxide": 0.0, "density": 0.0, "pH": 0.0, "sulphates": 0.0,
     "alcohol": 0.0, "quality": 0},
    # Maximum values for each feature
    {"fixed acidity": 15.0, "volatile acidity": 1.5, "citric acid": 1.0, "residual sugar": 65.0, "chlorides": 0.611,
     "free sulfur dioxide": 289.0, "total sulfur dioxide": 440.0, "density": 1.1, "pH": 4.0, "sulphates": 2.0,
     "alcohol": 14.9, "quality": 10}
]

# Error case test data (invalid inputs)
# These records contain invalid inputs to test error handling.
error_case_data = [
    # Negative values for features
    {"fixed acidity": -1.0, "volatile acidity": -0.1, "citric acid": -0.2, "residual sugar": -1.0, "chlorides": -0.01,
     "free sulfur dioxide": -5.0, "total sulfur dioxide": -10.0, "density": -0.99, "pH": -3.0, "sulphates": -0.5,
     "alcohol": -8.0, "quality": -1},
    # Non-numeric values for features
    {"fixed acidity": "NaN", "volatile acidity": "NaN", "citric acid": "NaN", "residual sugar": "NaN", "chlorides": "NaN",
     "free sulfur dioxide": "NaN", "total sulfur dioxide": "NaN", "density": "NaN", "pH": "NaN", "sulphates": "NaN",
     "alcohol": "NaN", "quality": "NaN"}
]

# Special character and format test data
# These records include special characters and unusual formats.
special_character_data = [
    # Special characters in numeric fields
    {"fixed acidity": "7.0$", "volatile acidity": "0.27@", "citric acid": "0.36#", "residual sugar": "20.7%", "chlorides": "0.045^",
     "free sulfur dioxide": "45.0&", "total sulfur dioxide": "170.0*", "density": "1.001(", "pH": "3.0)", "sulphates": "0.45_",
     "alcohol": "8.8+", "quality": "6!"},
    # Mixed data types
    {"fixed acidity": 7.0, "volatile acidity": "0.27", "citric acid": 0.36, "residual sugar": "20.7", "chlorides": 0.045,
     "free sulfur dioxide": "45.0", "total sulfur dioxide": 170.0, "density": "1.001", "pH": 3.0, "sulphates": "0.45",
     "alcohol": 8.8, "quality": "6"}
]

# Combine all test data into a single list
test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Convert test data to DataFrame
test_df = pd.DataFrame(test_data)

# Display the test data
print(test_df)

