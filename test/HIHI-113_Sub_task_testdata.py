import random
import string
import json
from datetime import datetime, timedelta

# Helper functions
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def random_email():
    return f"{random_string(5)}@{random_string(3)}.com"

def random_date(start_year=2000, end_year=2023):
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    return start_date + (end_date - start_date) * random.random()

def random_password():
    return ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation, k=12))

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid user data
    {
        "username": "validUser1",
        "email": "user1@example.com",
        "password": "ValidPass123!",
        "created_at": "2023-01-01T12:00:00Z"
    },
    {
        "username": "validUser2",
        "email": "user2@example.com",
        "password": "ValidPass456!",
        "created_at": "2023-02-01T12:00:00Z"
    }
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Minimum length username
    {
        "username": "u",
        "email": "minuser@example.com",
        "password": "MinPass123!",
        "created_at": "2023-03-01T12:00:00Z"
    },
    # Maximum length username
    {
        "username": random_string(50),
        "email": "maxuser@example.com",
        "password": "MaxPass123!",
        "created_at": "2023-04-01T12:00:00Z"
    }
]

# Error case test data (invalid inputs)
error_case_data = [
    # Invalid email format
    {
        "username": "invalidEmailUser",
        "email": "invalid-email",
        "password": "InvalidPass123!",
        "created_at": "2023-05-01T12:00:00Z"
    },
    # Missing password
    {
        "username": "missingPasswordUser",
        "email": "missingpass@example.com",
        "password": "",
        "created_at": "2023-06-01T12:00:00Z"
    }
]

# Special character and format test data
special_char_data = [
    # Username with special characters
    {
        "username": "user!@#",
        "email": "specialchar@example.com",
        "password": "SpecialPass123!",
        "created_at": "2023-07-01T12:00:00Z"
    },
    # Password with special characters
    {
        "username": "specialPasswordUser",
        "email": "specialpass@example.com",
        "password": "!@#$%^&*()_+",
        "created_at": "2023-08-01T12:00:00Z"
    }
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_char_data

# Output test data as JSON
print(json.dumps(all_test_data, indent=2))

