import random
import string
import json
from faker import Faker

fake = Faker()

# Helper functions
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def random_email():
    return fake.email()

def random_username():
    return fake.user_name()

def random_password():
    return fake.password()

def random_phone_number():
    return fake.phone_number()

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid user data
    {
        "username": random_username(),
        "password": random_password(),
        "email": random_email(),
        "phone": random_phone_number()
    } for _ in range(10)
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Minimum length username
    {
        "username": "a",
        "password": random_password(),
        "email": random_email(),
        "phone": random_phone_number()
    },
    # Maximum length username
    {
        "username": random_string(50),
        "password": random_password(),
        "email": random_email(),
        "phone": random_phone_number()
    },
    # Minimum length password
    {
        "username": random_username(),
        "password": "12345",
        "email": random_email(),
        "phone": random_phone_number()
    },
    # Maximum length password
    {
        "username": random_username(),
        "password": random_string(100),
        "email": random_email(),
        "phone": random_phone_number()
    }
]

# Error case test data (invalid inputs)
error_case_data = [
    # Invalid email format
    {
        "username": random_username(),
        "password": random_password(),
        "email": "invalid-email",
        "phone": random_phone_number()
    },
    # Missing username
    {
        "username": "",
        "password": random_password(),
        "email": random_email(),
        "phone": random_phone_number()
    },
    # Missing password
    {
        "username": random_username(),
        "password": "",
        "email": random_email(),
        "phone": random_phone_number()
    },
    # Invalid phone number
    {
        "username": random_username(),
        "password": random_password(),
        "email": random_email(),
        "phone": "123"
    }
]

# Special character and format test data
special_character_data = [
    # Username with special characters
    {
        "username": "user!@#",
        "password": random_password(),
        "email": random_email(),
        "phone": random_phone_number()
    },
    # Password with special characters
    {
        "username": random_username(),
        "password": "P@ssw0rd!",
        "email": random_email(),
        "phone": random_phone_number()
    },
    # Email with subdomain
    {
        "username": random_username(),
        "password": random_password(),
        "email": "user@sub.domain.com",
        "phone": random_phone_number()
    }
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Output the test data as JSON
print(json.dumps(all_test_data, indent=2))
