import random
import string
import json

# Helper functions
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_email():
    return f"{random_string(5)}@{random_string(3)}.com"

def random_phone():
    return f"+1{random.randint(1000000000, 9999999999)}"

# Happy Path Test Data
# Valid user data with expected scenarios
happy_path_data = [
    {
        "userId": 1,
        "name": "John Doe",
        "email": "john.doe@example.com",
        "phoneNumber": "+12345678901"
    },
    {
        "userId": 2,
        "name": "Jane Smith",
        "email": "jane.smith@example.com",
        "phoneNumber": "+19876543210"
    }
]

# Edge Case Test Data
# Boundary conditions for userId and phoneNumber
edge_case_data = [
    {
        "userId": 0,  # Minimum boundary for userId
        "name": "Edge Case User",
        "email": "edge.case@example.com",
        "phoneNumber": "+10000000000"  # Minimum valid phone number
    },
    {
        "userId": 2147483647,  # Maximum boundary for userId (assuming 32-bit integer)
        "name": "Max Int User",
        "email": "max.int@example.com",
        "phoneNumber": "+19999999999"  # Maximum valid phone number
    }
]

# Error Case Test Data
# Invalid inputs for email and phoneNumber
error_case_data = [
    {
        "userId": 3,
        "name": "Invalid Email User",
        "email": "invalid-email",  # Invalid email format
        "phoneNumber": "+12345678901"
    },
    {
        "userId": 4,
        "name": "Invalid Phone User",
        "email": "valid.email@example.com",
        "phoneNumber": "1234567890"  # Invalid phone format (missing country code)
    }
]

# Special Character and Format Test Data
# Testing special characters in name and email
special_character_data = [
    {
        "userId": 5,
        "name": "Special!@#User",
        "email": "special!@#user@example.com",
        "phoneNumber": "+12345678901"
    },
    {
        "userId": 6,
        "name": "Normal User",
        "email": "normal.user+test@example.com",  # Email with plus sign
        "phoneNumber": "+12345678901"
    }
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Output the test data in JSON format
print(json.dumps(all_test_data, indent=2))

