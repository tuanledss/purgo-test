import json
import random
import string

# Helper functions
def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def random_action():
    return random.choice(['create', 'update', 'delete', 'retrieve'])

def random_parameters():
    return {f"key{random.randint(1, 5)}": random_string() for _ in range(2)}

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid userId and action with parameters
    {"userId": random_string(), "action": random_action(), "parameters": random_parameters()},
    {"userId": random_string(), "action": random_action(), "parameters": random_parameters()},
    {"userId": random_string(), "action": random_action(), "parameters": random_parameters()},
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Minimum length userId
    {"userId": "", "action": random_action(), "parameters": random_parameters()},
    # Maximum length userId
    {"userId": random_string(255), "action": random_action(), "parameters": random_parameters()},
    # Empty parameters
    {"userId": random_string(), "action": random_action(), "parameters": {}},
]

# Error case test data (invalid inputs)
error_case_data = [
    # Missing userId
    {"action": random_action(), "parameters": random_parameters()},
    # Invalid action
    {"userId": random_string(), "action": "invalid_action", "parameters": random_parameters()},
    # Non-dictionary parameters
    {"userId": random_string(), "action": random_action(), "parameters": "not_a_dict"},
]

# Special character and format test data
special_character_data = [
    # Special characters in userId
    {"userId": "!@#$%^&*()", "action": random_action(), "parameters": random_parameters()},
    # Special characters in parameters
    {"userId": random_string(), "action": random_action(), "parameters": {"key1": "!@#$%^&*()"}},
    # JSON format with nested objects
    {"userId": random_string(), "action": random_action(), "parameters": {"key1": {"nestedKey": "nestedValue"}}},
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Output the test data
for i, test_case in enumerate(all_test_data, start=1):
    print(f"Test Case {i}: {json.dumps(test_case, indent=2)}")


This code generates test data for a system that processes JSON requests with `userId`, `action`, and `parameters`. It covers happy path scenarios, edge cases, error cases, and special character scenarios, ensuring a comprehensive test suite.