import uuid
import random
import string
import json

# Helper functions
def generate_uuid():
    return str(uuid.uuid4())

def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_email():
    return f"{generate_random_string(5)}@example.com"

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid userId and action
    {"userId": generate_uuid(), "action": "fetchData"},
    {"userId": generate_uuid(), "action": "updateData"},
    {"userId": generate_uuid(), "action": "deleteData"},
    {"userId": generate_uuid(), "action": "createData"},
    # Valid userId and action with different cases
    {"userId": generate_uuid(), "action": "FetchData"},
    {"userId": generate_uuid(), "action": "UPDATEDATA"},
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Minimum length userId (UUID length)
    {"userId": generate_uuid(), "action": "fetchData"},
    # Maximum length action (assuming 20 chars max)
    {"userId": generate_uuid(), "action": generate_random_string(20)},
]

# Error case test data (invalid inputs)
error_case_data = [
    # Invalid userId format
    {"userId": "12345", "action": "fetchData"},
    {"userId": "invalid-uuid", "action": "updateData"},
    # Invalid action
    {"userId": generate_uuid(), "action": "invalidAction"},
    # Missing userId
    {"action": "fetchData"},
    # Missing action
    {"userId": generate_uuid()},
]

# Special character and format test data
special_character_data = [
    # Special characters in action
    {"userId": generate_uuid(), "action": "fetch@Data"},
    {"userId": generate_uuid(), "action": "update#Data"},
    # Special characters in userId
    {"userId": "123e4567-e89b-12d3-a456-426614174000$", "action": "fetchData"},
    # JSON with special characters
    {"userId": generate_uuid(), "action": "fetchData", "extra": "!@#$%^&*()"},
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Output the test data
print(json.dumps(all_test_data, indent=2))

