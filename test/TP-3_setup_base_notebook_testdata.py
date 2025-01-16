# Import necessary libraries
import random
import string

# Function to generate random strings
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# Happy Path Test Data
# Valid scenarios where all inputs are expected to succeed
happy_path_data = [
    {"id": 1, "name": "John Doe", "email": "john.doe@example.com", "age": 30},  # Typical valid input
    {"id": 2, "name": "Jane Smith", "email": "jane.smith@example.com", "age": 25},  # Another valid input
    {"id": 3, "name": "Alice Johnson", "email": "alice.j@example.com", "age": 40},  # Valid input with different email format
]

# Edge Case Test Data
# Boundary conditions and limits
edge_case_data = [
    {"id": 4, "name": "A" * 50, "email": "a@example.com", "age": 18},  # Maximum name length
    {"id": 5, "name": "B" * 1, "email": "b@example.com", "age": 65},  # Minimum name length
    {"id": 6, "name": "Charlie", "email": "charlie@example.com", "age": 0},  # Minimum age
    {"id": 7, "name": "Delta", "email": "delta@example.com", "age": 120},  # Maximum age
]

# Error Case Test Data
# Invalid inputs that should trigger errors
error_case_data = [
    {"id": 8, "name": "", "email": "emptyname@example.com", "age": 30},  # Empty name
    {"id": 9, "name": "Eve", "email": "invalid-email", "age": 30},  # Invalid email format
    {"id": 10, "name": "Frank", "email": "frank@example.com", "age": -1},  # Negative age
    {"id": 11, "name": "Grace", "email": "grace@example.com", "age": 130},  # Age beyond maximum
]

# Special Character and Format Test Data
# Inputs with special characters and different formats
special_character_data = [
    {"id": 12, "name": "H@rry", "email": "harry@example.com", "age": 30},  # Name with special character
    {"id": 13, "name": "Ivy", "email": "ivy+test@example.com", "age": 30},  # Email with special character
    {"id": 14, "name": "Jack", "email": "jack@example.com", "age": 30},  # Normal input for control
    {"id": 15, "name": "K@te", "email": "kate@example.com", "age": 30},  # Another name with special character
]

# Combine all test data into a single list
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Print all test data
for record in all_test_data:
    print(record)
