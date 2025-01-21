import random
import string

# Helper functions
def random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_number(length):
    return ''.join(random.choices(string.digits, k=length))

def random_special_char_string(length):
    return ''.join(random.choices(string.punctuation, k=length))

# Happy Path Test Data
# Valid scenarios with expected inputs
happy_path_data = [
    {"name": "John Doe", "age": 30, "email": "john.doe@example.com"},  # Valid name, age, and email
    {"name": "Jane Smith", "age": 25, "email": "jane.smith@example.com"},  # Another valid entry
    {"name": "Alice Johnson", "age": 40, "email": "alice.j@example.com"},  # Valid with different email format
]

# Edge Case Test Data
# Boundary conditions
edge_case_data = [
    {"name": "A", "age": 0, "email": "a@example.com"},  # Minimum length name, minimum age
    {"name": random_string(255), "age": 120, "email": "max.name@example.com"},  # Maximum length name, maximum age
    {"name": "Bob", "age": 18, "email": "bob@example.com"},  # Minimum valid age
    {"name": "Charlie", "age": 65, "email": "charlie@example.com"},  # Typical retirement age
]

# Error Case Test Data
# Invalid inputs
error_case_data = [
    {"name": "", "age": 30, "email": "emptyname@example.com"},  # Empty name
    {"name": "Invalid Age", "age": -1, "email": "invalidage@example.com"},  # Negative age
    {"name": "No Email", "age": 25, "email": ""},  # Empty email
    {"name": "Invalid Email", "age": 25, "email": "invalidemail.com"},  # Missing '@' in email
    {"name": "Special Char Name", "age": 30, "email": "special@char.com"},  # Name with special characters
]

# Special Character and Format Test Data
# Testing special characters and formats
special_char_data = [
    {"name": "Name With Space", "age": 30, "email": "space@example.com"},  # Name with space
    {"name": "Name-With-Dash", "age": 30, "email": "dash@example.com"},  # Name with dash
    {"name": "Name_With_Underscore", "age": 30, "email": "underscore@example.com"},  # Name with underscore
    {"name": random_special_char_string(10), "age": 30, "email": "special@example.com"},  # Name with special characters
]

# Combine all test data
test_data = happy_path_data + edge_case_data + error_case_data + special_char_data

# Output the test data
for record in test_data:
    print(record)
