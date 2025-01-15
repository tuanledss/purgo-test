import pandas as pd
import random

# Constants for data generation
QUALITY_THRESHOLD = 7
NUM_RECORDS = 30

# Helper function to generate random float within a range
def random_float(min_val, max_val, precision=2):
    return round(random.uniform(min_val, max_val), precision)

# Happy path test data (valid, expected scenarios)
# These records are expected to pass all validations and represent typical data
happy_path_data = [
    {
        "fixed_acidity": random_float(6.0, 9.0),
        "volatile_acidity": random_float(0.2, 0.5),
        "citric_acid": random_float(0.2, 0.4),
        "residual_sugar": random_float(1.0, 3.0),
        "chlorides": random_float(0.03, 0.05),
        "free_sulfur_dioxide": random_float(20.0, 40.0),
        "total_sulfur_dioxide": random_float(100.0, 150.0),
        "density": random_float(0.990, 0.995),
        "pH": random_float(3.0, 3.5),
        "sulphates": random_float(0.4, 0.6),
        "alcohol": random_float(10.0, 12.0),
        "quality": random.randint(5, 8)
    } for _ in range(10)
]

# Edge case test data (boundary conditions)
# These records test the boundaries of valid input ranges
edge_case_data = [
    {
        "fixed_acidity": 6.0,  # Lower boundary
        "volatile_acidity": 0.2,  # Lower boundary
        "citric_acid": 0.2,  # Lower boundary
        "residual_sugar": 1.0,  # Lower boundary
        "chlorides": 0.03,  # Lower boundary
        "free_sulfur_dioxide": 20.0,  # Lower boundary
        "total_sulfur_dioxide": 100.0,  # Lower boundary
        "density": 0.990,  # Lower boundary
        "pH": 3.0,  # Lower boundary
        "sulphates": 0.4,  # Lower boundary
        "alcohol": 10.0,  # Lower boundary
        "quality": 5  # Lower boundary
    },
    {
        "fixed_acidity": 9.0,  # Upper boundary
        "volatile_acidity": 0.5,  # Upper boundary
        "citric_acid": 0.4,  # Upper boundary
        "residual_sugar": 3.0,  # Upper boundary
        "chlorides": 0.05,  # Upper boundary
        "free_sulfur_dioxide": 40.0,  # Upper boundary
        "total_sulfur_dioxide": 150.0,  # Upper boundary
        "density": 0.995,  # Upper boundary
        "pH": 3.5,  # Upper boundary
        "sulphates": 0.6,  # Upper boundary
        "alcohol": 12.0,  # Upper boundary
        "quality": 8  # Upper boundary
    }
]

# Error case test data (invalid inputs)
# These records are expected to fail validation checks
error_case_data = [
    {
        "fixed_acidity": -1.0,  # Invalid negative value
        "volatile_acidity": 0.5,
        "citric_acid": 0.3,
        "residual_sugar": 2.0,
        "chlorides": 0.04,
        "free_sulfur_dioxide": 30.0,
        "total_sulfur_dioxide": 120.0,
        "density": 0.992,
        "pH": 3.2,
        "sulphates": 0.5,
        "alcohol": 11.0,
        "quality": 6
    },
    {
        "fixed_acidity": 7.0,
        "volatile_acidity": 0.5,
        "citric_acid": 0.3,
        "residual_sugar": 2.0,
        "chlorides": 0.04,
        "free_sulfur_dioxide": 30.0,
        "total_sulfur_dioxide": 120.0,
        "density": 0.992,
        "pH": 3.2,
        "sulphates": 0.5,
        "alcohol": 11.0,
        "quality": 10  # Invalid quality value
    }
]

# Special character and format test data
# These records test the handling of special characters and formats
special_character_data = [
    {
        "fixed_acidity": "7.0",  # String instead of float
        "volatile_acidity": 0.3,
        "citric_acid": 0.3,
        "residual_sugar": 2.0,
        "chlorides": 0.04,
        "free_sulfur_dioxide": 30.0,
        "total_sulfur_dioxide": 120.0,
        "density": 0.992,
        "pH": 3.2,
        "sulphates": 0.5,
        "alcohol": 11.0,
        "quality": 6
    },
    {
        "fixed_acidity": 7.0,
        "volatile_acidity": 0.3,
        "citric_acid": 0.3,
        "residual_sugar": 2.0,
        "chlorides": 0.04,
        "free_sulfur_dioxide": 30.0,
        "total_sulfur_dioxide": 120.0,
        "density": 0.992,
        "pH": 3.2,
        "sulphates": 0.5,
        "alcohol": 11.0,
        "quality": "6"  # String instead of integer
    }
]

# Combine all test data into a single list
test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Convert test data to DataFrame
test_df = pd.DataFrame(test_data)

# Display the generated test data
print(test_df)
