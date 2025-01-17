import random
import string
import json

# Helper functions
def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def random_special_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation, k=length))

def random_username():
    return random_string(6)

def random_password():
    return random_string(10)

def random_special_password():
    return random_special_string(10)

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid username and password
    {"username": "user123", "password": "pass123456"},
    {"username": "johnDoe", "password": "securePass1"},
    {"username": random_username(), "password": random_password()},
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Minimum length username and password
    {"username": "u", "password": "p"},
    # Maximum length username and password
    {"username": random_string(255), "password": random_string(255)},
]

# Error case test data (invalid inputs)
error_case_data = [
    # Missing username
    {"password": "pass123456"},
    # Missing password
    {"username": "user123"},
    # Empty username and password
    {"username": "", "password": ""},
    # Non-string username and password
    {"username": 12345, "password": 67890},
]

# Special character and format test data
special_character_data = [
    # Username and password with special characters
    {"username": "user!@#", "password": "pass!@#123"},
    {"username": random_special_string(6), "password": random_special_password()},
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Output test data as JSON
print(json.dumps(all_test_data, indent=2))


This code generates test data for a system that requires a username and password, covering various scenarios such as valid inputs, edge cases, error cases, and special character handling. Each test case is structured to test specific conditions and validation rules.