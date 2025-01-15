import random
import string
import json

# Helper functions
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_email():
    return f"{random_string(5)}@{random_string(3)}.com"

def random_phone():
    return f"+1-{random.randint(100, 999)}-{random.randint(1000, 9999)}"

def random_date():
    return f"{random.randint(2000, 2023)}-{random.randint(1, 12):02}-{random.randint(1, 28):02}"

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid user data
    {"username": "john_doe", "email": "john.doe@example.com", "phone": "+1-555-1234", "dob": "1990-01-01"},
    {"username": "jane_smith", "email": "jane.smith@example.com", "phone": "+1-555-5678", "dob": "1985-05-15"},
    {"username": "alice_wonder", "email": "alice.wonder@example.com", "phone": "+1-555-8765", "dob": "1992-07-20"},
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Minimum length username
    {"username": "a", "email": random_email(), "phone": random_phone(), "dob": random_date()},
    # Maximum length username
    {"username": random_string(50), "email": random_email(), "phone": random_phone(), "dob": random_date()},
    # Leap year date of birth
    {"username": "leap_year", "email": random_email(), "phone": random_phone(), "dob": "2000-02-29"},
]

# Error case test data (invalid inputs)
error_case_data = [
    # Invalid email format
    {"username": "invalid_email", "email": "invalid-email", "phone": random_phone(), "dob": random_date()},
    # Invalid phone number format
    {"username": "invalid_phone", "email": random_email(), "phone": "12345", "dob": random_date()},
    # Future date of birth
    {"username": "future_dob", "email": random_email(), "phone": random_phone(), "dob": "2030-01-01"},
]

# Special character and format test data
special_char_data = [
    # Username with special characters
    {"username": "user!@#", "email": random_email(), "phone": random_phone(), "dob": random_date()},
    # Email with special characters
    {"username": "special_email", "email": "special!@example.com", "phone": random_phone(), "dob": random_date()},
    # Phone with special characters
    {"username": "special_phone", "email": random_email(), "phone": "+1-555-!@#$", "dob": random_date()},
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_char_data

# Output test data as JSON
print(json.dumps(all_test_data, indent=2))

