import random
import string
import json
from faker import Faker

fake = Faker()

# Helper functions
def random_email():
    return fake.email()

def random_phone_number():
    return fake.phone_number()

def random_user_id():
    return fake.uuid4()

def random_name():
    return fake.name()

def random_message():
    return fake.sentence()

def random_special_char_string(length=10):
    return ''.join(random.choices(string.punctuation, k=length))

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid user data
    {
        "userId": random_user_id(),
        "name": random_name(),
        "email": random_email(),
        "phoneNumber": random_phone_number()
    } for _ in range(10)
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Minimum length name
    {
        "userId": random_user_id(),
        "name": "A",
        "email": random_email(),
        "phoneNumber": random_phone_number()
    },
    # Maximum length name
    {
        "userId": random_user_id(),
        "name": "A" * 255,
        "email": random_email(),
        "phoneNumber": random_phone_number()
    },
    # Minimum length email
    {
        "userId": random_user_id(),
        "name": random_name(),
        "email": "a@b.co",
        "phoneNumber": random_phone_number()
    },
    # Maximum length email
    {
        "userId": random_user_id(),
        "name": random_name(),
        "email": "a" * 64 + "@example.com",
        "phoneNumber": random_phone_number()
    }
]

# Error case test data (invalid inputs)
error_case_data = [
    # Invalid email format
    {
        "userId": random_user_id(),
        "name": random_name(),
        "email": "invalid-email",
        "phoneNumber": random_phone_number()
    },
    # Invalid phone number format
    {
        "userId": random_user_id(),
        "name": random_name(),
        "email": random_email(),
        "phoneNumber": "12345"
    },
    # Missing required fields
    {
        "userId": random_user_id(),
        "name": random_name(),
        "email": random_email()
        # Missing phoneNumber
    }
]

# Special character and format test data
special_char_data = [
    # Name with special characters
    {
        "userId": random_user_id(),
        "name": random_special_char_string(),
        "email": random_email(),
        "phoneNumber": random_phone_number()
    },
    # Email with special characters
    {
        "userId": random_user_id(),
        "name": random_name(),
        "email": f"user{random_special_char_string()}@example.com",
        "phoneNumber": random_phone_number()
    }
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_char_data

# Output the test data as JSON
print(json.dumps(all_test_data, indent=2))

