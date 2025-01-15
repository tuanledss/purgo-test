import pandas as pd
import random

# Happy path test data (valid, expected scenarios)
# These records represent typical, valid entries for the wine quality dataset.
happy_path_data = [
    {"fixed_acidity": 7.0, "volatile_acidity": 0.27, "citric_acid": 0.36, "residual_sugar": 20.7, "chlorides": 0.045,
     "free_sulfur_dioxide": 45, "total_sulfur_dioxide": 170, "density": 1.001, "pH": 3.0, "sulphates": 0.45,
     "alcohol": 8.8, "quality": 6},  # Typical medium quality wine
    {"fixed_acidity": 6.3, "volatile_acidity": 0.3, "citric_acid": 0.34, "residual_sugar": 1.6, "chlorides": 0.049,
     "free_sulfur_dioxide": 14, "total_sulfur_dioxide": 132, "density": 0.994, "pH": 3.3, "sulphates": 0.49,
     "alcohol": 9.5, "quality": 6},  # Another medium quality wine
    {"fixed_acidity": 8.1, "volatile_acidity": 0.28, "citric_acid": 0.4, "residual_sugar": 6.9, "chlorides": 0.05,
     "free_sulfur_dioxide": 30, "total_sulfur_dioxide": 97, "density": 0.9951, "pH": 3.26, "sulphates": 0.44,
     "alcohol": 10.1, "quality": 6},  # High quality wine
]

# Edge case test data (boundary conditions)
# These records test the boundaries of the dataset's valid range.
edge_case_data = [
    {"fixed_acidity": 3.0, "volatile_acidity": 0.08, "citric_acid": 0.0, "residual_sugar": 0.6, "chlorides": 0.009,
     "free_sulfur_dioxide": 1, "total_sulfur_dioxide": 6, "density": 0.990, "pH": 2.72, "sulphates": 0.22,
     "alcohol": 8.0, "quality": 3},  # Minimum values for most fields
    {"fixed_acidity": 15.9, "volatile_acidity": 1.58, "citric_acid": 1.66, "residual_sugar": 65.8, "chlorides": 0.611,
     "free_sulfur_dioxide": 289, "total_sulfur_dioxide": 440, "density": 1.038, "pH": 4.01, "sulphates": 2.0,
     "alcohol": 14.9, "quality": 9},  # Maximum values for most fields
]

# Error case test data (invalid inputs)
# These records contain invalid data to test error handling.
error_case_data = [
    {"fixed_acidity": -1.0, "volatile_acidity": 0.27, "citric_acid": 0.36, "residual_sugar": 20.7, "chlorides": 0.045,
     "free_sulfur_dioxide": 45, "total_sulfur_dioxide": 170, "density": 1.001, "pH": 3.0, "sulphates": 0.45,
     "alcohol": 8.8, "quality": 6},  # Negative fixed acidity
    {"fixed_acidity": 7.0, "volatile_acidity": 0.27, "citric_acid": 0.36, "residual_sugar": 20.7, "chlorides": 0.045,
     "free_sulfur_dioxide": 45, "total_sulfur_dioxide": 170, "density": 1.001, "pH": 3.0, "sulphates": 0.45,
     "alcohol": 8.8, "quality": 12},  # Quality out of range
]

# Special character and format test data
# These records include special characters and unusual formats.
special_character_data = [
    {"fixed_acidity": 7.0, "volatile_acidity": 0.27, "citric_acid": 0.36, "residual_sugar": 20.7, "chlorides": 0.045,
     "free_sulfur_dioxide": 45, "total_sulfur_dioxide": 170, "density": 1.001, "pH": 3.0, "sulphates": 0.45,
     "alcohol": 8.8, "quality": "6*"},  # Special character in quality
    {"fixed_acidity": "7.0", "volatile_acidity": "0.27", "citric_acid": "0.36", "residual_sugar": "20.7",
     "chlorides": "0.045", "free_sulfur_dioxide": "45", "total_sulfur_dioxide": "170", "density": "1.001",
     "pH": "3.0", "sulphates": "0.45", "alcohol": "8.8", "quality": "6"},  # All fields as strings
]

# Combine all test data into a single list
test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Convert test data to DataFrame
test_df = pd.DataFrame(test_data)

# Display the test data
print(test_df)
