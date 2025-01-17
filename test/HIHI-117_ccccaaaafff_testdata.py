import pandas as pd
import random

# Define the columns based on the CSV structure
columns = [
    "fixed acidity", "volatile acidity", "citric acid", "residual sugar",
    "chlorides", "free sulfur dioxide", "total sulfur dioxide", "density",
    "pH", "sulphates", "alcohol", "quality"
]

# Function to generate random float within a range
def random_float(min_val, max_val, decimals=2):
    return round(random.uniform(min_val, max_val), decimals)

# Function to generate random integer within a range
def random_int(min_val, max_val):
    return random.randint(min_val, max_val)

# Happy path test data: Valid and expected scenarios
happy_path_data = [
    # Valid data within typical ranges
    {col: random_float(5.0, 10.0) for col in columns[:-1]} | {"quality": random_int(3, 8)}
    for _ in range(10)
]

# Edge case test data: Boundary conditions
edge_case_data = [
    # Minimum boundary values
    {col: 5.0 for col in columns[:-1]} | {"quality": 3},
    # Maximum boundary values
    {col: 10.0 for col in columns[:-1]} | {"quality": 8},
    # Specific edge cases for each column
    {"fixed acidity": 5.0, "volatile acidity": 0.1, "citric acid": 0.0, "residual sugar": 0.1,
     "chlorides": 0.01, "free sulfur dioxide": 1, "total sulfur dioxide": 1, "density": 0.990,
     "pH": 2.8, "sulphates": 0.1, "alcohol": 8.0, "quality": 3},
    {"fixed acidity": 15.0, "volatile acidity": 1.5, "citric acid": 1.0, "residual sugar": 20.0,
     "chlorides": 0.5, "free sulfur dioxide": 100, "total sulfur dioxide": 300, "density": 1.1,
     "pH": 4.0, "sulphates": 2.0, "alcohol": 15.0, "quality": 9}
]

# Error case test data: Invalid inputs
error_case_data = [
    # Negative values
    {"fixed acidity": -1.0, "volatile acidity": -0.1, "citric acid": -0.1, "residual sugar": -0.1,
     "chlorides": -0.01, "free sulfur dioxide": -1, "total sulfur dioxide": -1, "density": -0.990,
     "pH": -2.8, "sulphates": -0.1, "alcohol": -8.0, "quality": -3},
    # Non-numeric values
    {"fixed acidity": "NaN", "volatile acidity": "NaN", "citric acid": "NaN", "residual sugar": "NaN",
     "chlorides": "NaN", "free sulfur dioxide": "NaN", "total sulfur dioxide": "NaN", "density": "NaN",
     "pH": "NaN", "sulphates": "NaN", "alcohol": "NaN", "quality": "NaN"}
]

# Special character and format test data
special_character_data = [
    # Special characters in numeric fields
    {"fixed acidity": "5.0$", "volatile acidity": "0.3@", "citric acid": "0.2#", "residual sugar": "1.0%",
     "chlorides": "0.05&", "free sulfur dioxide": "30*", "total sulfur dioxide": "100^", "density": "0.995!",
     "pH": "3.3~", "sulphates": "0.5`", "alcohol": "10.0|", "quality": "6"},
    # Mixed format data
    {"fixed acidity": "5,0", "volatile acidity": "0.3", "citric acid": "0.2", "residual sugar": "1.0",
     "chlorides": "0.05", "free sulfur dioxide": "30", "total sulfur dioxide": "100", "density": "0.995",
     "pH": "3.3", "sulphates": "0.5", "alcohol": "10.0", "quality": "6"}
]

# Combine all test data
test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Convert to DataFrame
df_test_data = pd.DataFrame(test_data, columns=columns)

# Display the test data
print(df_test_data)


This code generates test data for a wine quality dataset, covering happy path, edge cases, error cases, and special character scenarios. Each test case is designed to validate different aspects of the data processing pipeline.