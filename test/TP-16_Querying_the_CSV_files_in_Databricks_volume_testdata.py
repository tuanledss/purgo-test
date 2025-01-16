import random
import string
import pandas as pd

# Happy Path Test Data
# These records represent valid and expected scenarios
happy_path_data = [
    {"product_id": 1, "product_name": "Widget A", "revenue": 1000.00, "currency": "USD"},
    {"product_id": 2, "product_name": "Widget B", "revenue": 1500.50, "currency": "USD"},
    {"product_id": 3, "product_name": "Widget C", "revenue": 2000.75, "currency": "USD"},
    {"product_id": 4, "product_name": "Widget D", "revenue": 2500.00, "currency": "USD"},
    {"product_id": 5, "product_name": "Widget E", "revenue": 3000.25, "currency": "USD"},
]

# Edge Case Test Data
# These records test boundary conditions
edge_case_data = [
    {"product_id": 0, "product_name": "Widget F", "revenue": 0.00, "currency": "USD"},  # Minimum values
    {"product_id": 999999, "product_name": "Widget G", "revenue": 999999.99, "currency": "USD"},  # Maximum values
    {"product_id": 6, "product_name": "Widget H", "revenue": 0.01, "currency": "USD"},  # Smallest non-zero revenue
    {"product_id": 7, "product_name": "Widget I", "revenue": 999999.98, "currency": "USD"},  # Just below max revenue
]

# Error Case Test Data
# These records test invalid inputs
error_case_data = [
    {"product_id": -1, "product_name": "Widget J", "revenue": 100.00, "currency": "USD"},  # Negative product_id
    {"product_id": 8, "product_name": "Widget K", "revenue": -100.00, "currency": "USD"},  # Negative revenue
    {"product_id": 9, "product_name": "", "revenue": 500.00, "currency": "USD"},  # Empty product name
    {"product_id": 10, "product_name": "Widget L", "revenue": 100.00, "currency": "XYZ"},  # Invalid currency
]

# Special Character and Format Test Data
# These records test special characters and formats
special_character_data = [
    {"product_id": 11, "product_name": "Widget M", "revenue": 100.00, "currency": "USD"},
    {"product_id": 12, "product_name": "Widget N", "revenue": 100.00, "currency": "USD"},
    {"product_id": 13, "product_name": "Widget O", "revenue": 100.00, "currency": "USD"},
    {"product_id": 14, "product_name": "Widget P", "revenue": 100.00, "currency": "USD"},
    {"product_id": 15, "product_name": "Widget Q", "revenue": 100.00, "currency": "USD"},
]

# Combine all test data into a single list
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Convert to DataFrame for easy manipulation and export
df = pd.DataFrame(all_test_data)

# Output the test data to a CSV file
df.to_csv('test_data.csv', index=False)


This code generates a set of test data records across different categories, including happy path, edge cases, error cases, and special character scenarios. Each record is structured to test specific conditions and validation rules. The data is then combined into a single DataFrame and exported to a CSV file for use in testing.