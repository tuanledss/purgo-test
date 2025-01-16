import random
import string
import json
import re

# Helper functions
def random_string(length, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for _ in range(length))

def random_email():
    return f"{random_string(5)}@{random_string(5)}.com"

def random_password():
    return random_string(6) + random.choice(string.digits) + random.choice(string.punctuation)

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid user data
    {"username": "user123", "password": "Passw0rd!", "email": "user123@example.com"},
    {"username": "johnDoe", "password": "Secure1@", "email": "john.doe@example.com"},
    {"username": "alice2023", "password": "Alice@2023", "email": "alice@example.com"},
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Username at minimum length
    {"username": "user1", "password": "Valid1@", "email": "user1@example.com"},
    # Username at maximum length
    {"username": "user1234567890", "password": "Valid1@", "email": "user1234567890@example.com"},
    # Password at minimum length
    {"username": "edgeUser", "password": "Pass1@", "email": "edge@example.com"},
]

# Error case test data (invalid inputs)
error_case_data = [
    # Invalid username (too short)
    {"username": "usr", "password": "Valid1@", "email": "usr@example.com"},
    # Invalid username (too long)
    {"username": "user1234567890123", "password": "Valid1@", "email": "user1234567890123@example.com"},
    # Invalid password (no special character)
    {"username": "invalidUser", "password": "Password1", "email": "invalid@example.com"},
    # Invalid email format
    {"username": "invalidEmail", "password": "Valid1@", "email": "invalidemail.com"},
]

# Special character and format test data
special_char_data = [
    # Username with special characters
    {"username": "user!@#", "password": "Valid1@", "email": "user!@#@example.com"},
    # Password with multiple special characters
    {"username": "specialUser", "password": "P@ssw0rd!#", "email": "special@example.com"},
    # Email with subdomain
    {"username": "subdomainUser", "password": "Valid1@", "email": "user@sub.example.com"},
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_char_data

# Output test data as JSON
print(json.dumps(all_test_data, indent=2))


This code generates test data for a system with user authentication and validation rules. It includes happy path scenarios, edge cases, error cases, and special character scenarios, ensuring comprehensive coverage of the specified requirements.