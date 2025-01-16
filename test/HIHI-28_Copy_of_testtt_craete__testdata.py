import random
import string
import json
from datetime import datetime, timedelta

# Helper functions
def random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_email():
    return f"{random_string(5)}@{random_string(3)}.com"

def random_password(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation, k=length))

def random_timestamp():
    return (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat()

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid user data
    {"userId": 1, "name": "John Doe", "email": "john.doe@example.com", "password": "SecurePass123!"},
    {"userId": 2, "name": "Jane Smith", "email": "jane.smith@example.com", "password": "AnotherPass456@"},
    # Valid processed data
    {"transactionId": 101, "status": "completed", "timestamp": random_timestamp()},
    {"transactionId": 102, "status": "pending", "timestamp": random_timestamp()},
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Minimum length password
    {"userId": 3, "name": "Edge Case", "email": "edge.case@example.com", "password": random_password(8)},
    # Maximum length name
    {"userId": 4, "name": random_string(255), "email": "max.name@example.com", "password": "MaxNamePass789#"},
    # Future timestamp
    {"transactionId": 103, "status": "scheduled", "timestamp": (datetime.now() + timedelta(days=1)).isoformat()},
]

# Error case test data (invalid inputs)
error_case_data = [
    # Invalid email format
    {"userId": 5, "name": "Invalid Email", "email": "invalid-email", "password": "InvalidEmailPass!"},
    # Password too short
    {"userId": 6, "name": "Short Password", "email": "short.pass@example.com", "password": "short"},
    # Missing userId
    {"name": "Missing UserId", "email": "missing.userid@example.com", "password": "MissingUserIdPass!"},
]

# Special character and format test data
special_character_data = [
    # Special characters in name
    {"userId": 7, "name": "Special!@#$%^&*()_+", "email": "special.char@example.com", "password": "SpecialCharPass!"},
    # Email with subdomain
    {"userId": 8, "name": "Subdomain Email", "email": "sub.domain@sub.example.com", "password": "SubDomainPass123!"},
    # Password with all special characters
    {"userId": 9, "name": "All Special", "email": "all.special@example.com", "password": "!@#$%^&*()_+"},
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Output test data as JSON
print(json.dumps(all_test_data, indent=2))

