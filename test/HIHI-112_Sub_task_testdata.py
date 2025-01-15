import random
import string
import json
import re

# Helper functions
def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_email():
    return f"{random_string()}@example.com"

def random_role():
    return random.choice(['admin', 'user', 'guest'])

def is_valid_email(email):
    return re.match(r"[^@]+@[^@]+\.[^@]+", email) is not None

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid user data
    {"userId": "user123", "name": "John Doe", "email": "john.doe@example.com", "role": "admin"},
    {"userId": "user456", "name": "Jane Smith", "email": "jane.smith@example.com", "role": "user"},
    {"userId": "user789", "name": "Alice Johnson", "email": "alice.j@example.com", "role": "guest"},
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Minimum length userId
    {"userId": "u", "name": "Min User", "email": "min.user@example.com", "role": "user"},
    # Maximum length userId
    {"userId": random_string(255), "name": "Max User", "email": "max.user@example.com", "role": "admin"},
    # Valid email with subdomain
    {"userId": "userSub", "name": "Subdomain User", "email": "user@sub.example.com", "role": "guest"},
]

# Error case test data (invalid inputs)
error_case_data = [
    # Empty userId
    {"userId": "", "name": "Empty UserId", "email": "empty.userid@example.com", "role": "user"},
    # Invalid email format
    {"userId": "invalidEmail", "name": "Invalid Email", "email": "invalid-email", "role": "admin"},
    # Unsupported role
    {"userId": "unsupportedRole", "name": "Unsupported Role", "email": "unsupported.role@example.com", "role": "superuser"},
]

# Special character and format test data
special_character_data = [
    # UserId with special characters
    {"userId": "user!@#", "name": "Special Char User", "email": "special.char@example.com", "role": "user"},
    # Name with special characters
    {"userId": "specialName", "name": "Name!@#", "email": "name.special@example.com", "role": "guest"},
    # Email with special characters
    {"userId": "specialEmail", "name": "Special Email", "email": "special.email+test@example.com", "role": "admin"},
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Validate and print test data
for data in all_test_data:
    assert isinstance(data['userId'], str) and data['userId'], "Invalid userId"
    assert isinstance(data['name'], str) and data['name'], "Invalid name"
    assert is_valid_email(data['email']), "Invalid email format"
    assert data['role'] in ['admin', 'user', 'guest'], "Invalid role"
    print(json.dumps(data, indent=2))
