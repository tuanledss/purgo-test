import uuid
import random
import string
import json

# Helper function to generate random strings
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# Helper function to generate a valid UUID
def generate_uuid():
    return str(uuid.uuid4())

# Happy Path Test Data
# Valid userId and action
happy_path_data = [
    {"userId": generate_uuid(), "action": "fetchData"},
    {"userId": generate_uuid(), "action": "updateData"},
    {"userId": generate_uuid(), "action": "deleteData"},
]

# Edge Case Test Data
# Boundary conditions for userId and action
edge_case_data = [
    {"userId": generate_uuid(), "action": ""},  # Empty action
    {"userId": "", "action": "fetchData"},  # Empty userId
    {"userId": generate_uuid(), "action": "a" * 255},  # Long action string
    {"userId": "0" * 36, "action": "fetchData"},  # userId with all zeros
]

# Error Case Test Data
# Invalid inputs for userId and action
error_case_data = [
    {"userId": "invalid-uuid", "action": "fetchData"},  # Invalid UUID format
    {"userId": generate_uuid(), "action": "invalidAction"},  # Unrecognized action
    {"userId": "12345", "action": "fetchData"},  # Non-UUID userId
    {"userId": generate_uuid(), "action": None},  # None action
]

# Special Character and Format Test Data
# Test special characters and formats in userId and action
special_character_data = [
    {"userId": generate_uuid(), "action": "fetchData!@#$%^&*()"},  # Special characters in action
    {"userId": generate_uuid(), "action": "fetchData\n"},  # Newline in action
    {"userId": generate_uuid(), "action": "fetchData\t"},  # Tab in action
    {"userId": "123e4567-e89b-12d3-a456-426614174000", "action": "fetchData"},  # Valid UUID with special format
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Output the test data as JSON
print(json.dumps(all_test_data, indent=2))


This code generates test data for a system that processes user actions with a `userId` and `action` field. It covers happy path scenarios, edge cases, error cases, and special character scenarios, ensuring comprehensive testing of the system's input validation and processing logic.