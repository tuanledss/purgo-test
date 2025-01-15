import json
import random
import string

# Helper functions
def random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_email():
    return f"{random_string(5)}@{random_string(3)}.com"

def random_password(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation, k=length))

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid user data
    {"username": "validUser1", "email": "user1@example.com", "password": "Password123!"},
    {"username": "validUser2", "email": "user2@example.com", "password": "SecurePass!2"},
    {"username": "validUser3", "email": "user3@example.com", "password": "AnotherPass#3"},
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Minimum length password
    {"username": "edgeUser1", "email": "edge1@example.com", "password": "Pass123!"},
    # Maximum length username (assuming max length is 20)
    {"username": "u" * 20, "email": "edge2@example.com", "password": "ValidPass123!"},
    # Maximum length email (assuming max length is 50)
    {"username": "edgeUser3", "email": f"{'e'*40}@example.com", "password": "ValidPass123!"},
]

# Error case test data (invalid inputs)
error_case_data = [
    # Missing username
    {"email": "error1@example.com", "password": "ErrorPass123!"},
    # Invalid email format
    {"username": "errorUser2", "email": "invalid-email", "password": "ErrorPass123!"},
    # Password too short
    {"username": "errorUser3", "email": "error3@example.com", "password": "short"},
]

# Special character and format test data
special_character_data = [
    # Username with special characters
    {"username": "user!@#", "email": "special1@example.com", "password": "SpecialPass123!"},
    # Email with subdomain
    {"username": "specialUser2", "email": "user@sub.example.com", "password": "SpecialPass123!"},
    # Password with all special characters
    {"username": "specialUser3", "email": "special3@example.com", "password": "!@#$%^&*()_+"},
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Output test data as JSON
print(json.dumps(all_test_data, indent=2))


This code generates test data for a system that requires a username, email, and password. It covers happy path scenarios, edge cases, error cases, and special character scenarios, ensuring a comprehensive set of test data for validation.