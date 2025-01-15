import uuid
import random
import json

# Helper function to generate a valid UUID
def generate_valid_uuid():
    return str(uuid.uuid4())

# Helper function to generate an invalid UUID
def generate_invalid_uuid():
    return "invalid-uuid"

# Helper function to generate a valid action
def generate_valid_action():
    return random.choice(["create", "update"])

# Helper function to generate an invalid action
def generate_invalid_action():
    return "delete"

# Helper function to generate a valid positive integer
def generate_valid_positive_integer():
    return random.randint(1, 100)

# Helper function to generate an invalid integer (negative)
def generate_invalid_integer():
    return random.randint(-100, -1)

# Helper function to generate special character strings
def generate_special_character_string():
    return "!@#$%^&*()"

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid userId, action, and positive integer
    {
        "userId": generate_valid_uuid(),
        "action": generate_valid_action(),
        "data": {
            "field1": "ValidString",
            "field2": generate_valid_positive_integer()
        }
    }
    for _ in range(10)
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Valid userId, action, and zero as boundary condition
    {
        "userId": generate_valid_uuid(),
        "action": generate_valid_action(),
        "data": {
            "field1": "EdgeCaseString",
            "field2": 0  # Boundary condition
        }
    }
]

# Error case test data (invalid inputs)
error_case_data = [
    # Invalid userId
    {
        "userId": generate_invalid_uuid(),
        "action": generate_valid_action(),
        "data": {
            "field1": "ErrorCaseString",
            "field2": generate_valid_positive_integer()
        }
    },
    # Invalid action
    {
        "userId": generate_valid_uuid(),
        "action": generate_invalid_action(),
        "data": {
            "field1": "ErrorCaseString",
            "field2": generate_valid_positive_integer()
        }
    },
    # Invalid negative integer
    {
        "userId": generate_valid_uuid(),
        "action": generate_valid_action(),
        "data": {
            "field1": "ErrorCaseString",
            "field2": generate_invalid_integer()
        }
    }
]

# Special character and format test data
special_character_data = [
    # Special characters in field1
    {
        "userId": generate_valid_uuid(),
        "action": generate_valid_action(),
        "data": {
            "field1": generate_special_character_string(),
            "field2": generate_valid_positive_integer()
        }
    }
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Output the test data in JSON format
print(json.dumps(all_test_data, indent=2))

