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
    # Username at boundary length
    {"username": "user1", "password": "Passw0rd!", "email": "user1@example.com"},  # Min length
    {"username": "user1234567890", "password": "Passw0rd!", "email": "user1234567890@example.com"},  # Max length
    # Password at boundary length
    {"username": "edgeUser", "password": "P@ssw0r", "email": "edge@example.com"},  # Min length
]

# Error case test data (invalid inputs)
error_case_data = [
    # Invalid username
    {"username": "us", "password": "Passw0rd!", "email": "us@example.com"},  # Too short
    {"username": "user12345678901", "password": "Passw0rd!", "email": "user12345678901@example.com"},  # Too long
    # Invalid password
    {"username": "user123", "password": "password", "email": "user123@example.com"},  # No special char or number
    {"username": "user123", "password": "12345678", "email": "user123@example.com"},  # No letter
    # Invalid email
    {"username": "user123", "password": "Passw0rd!", "email": "user123example.com"},  # Missing '@'
    {"username": "user123", "password": "Passw0rd!", "email": "user123@.com"},  # Missing domain
]

# Special character and format test data
special_char_data = [
    # Special characters in username
    {"username": "user!@#", "password": "Passw0rd!", "email": "user!@#@example.com"},
    # Special characters in email
    {"username": "user123", "password": "Passw0rd!", "email": "user123@ex!ample.com"},
    # Special characters in password
    {"username": "user123", "password": "P@ssw0rd!@#", "email": "user123@example.com"},
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_char_data

# Output test data
for i, data in enumerate(all_test_data, start=1):
    print(f"Test Case {i}: {json.dumps(data)}")


This code generates test data for a system with user authentication and validation rules. It covers happy path scenarios, edge cases, error cases, and special character scenarios, ensuring comprehensive test coverage.