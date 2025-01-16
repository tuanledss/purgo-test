# Import necessary libraries
import random
import string

# Happy Path Test Data
# Test data for valid, expected scenarios
happy_path_data = [
    # Valid scenario: Simple print statement
    {"description": "Basic print statement", "code": 'print("Hello, World!")'},
    # Valid scenario: Print with single quotes
    {"description": "Print with single quotes", "code": "print('Hello, World!')"},
    # Valid scenario: Print with variable
    {"description": "Print using a variable", "code": 'message = "Hello, World!"\nprint(message)'},
]

# Edge Case Test Data
# Test data for boundary conditions
edge_case_data = [
    # Edge case: Empty string
    {"description": "Print empty string", "code": 'print("")'},
    # Edge case: Long string
    {"description": "Print long string", "code": f'print("{ "Hello, World! " * 1000 }")'},
]

# Error Case Test Data
# Test data for invalid inputs
error_case_data = [
    # Error case: Missing parentheses (Python 3.x)
    {"description": "Missing parentheses", "code": 'print "Hello, World!"'},
    # Error case: Unmatched quotes
    {"description": "Unmatched quotes", "code": 'print("Hello, World!)'},
    # Error case: Invalid syntax
    {"description": "Invalid syntax", "code": 'print(Hello, World!)'},
]

# Special Character and Format Test Data
# Test data for special characters and formats
special_character_data = [
    # Special character: Newline character
    {"description": "Print with newline character", "code": 'print("Hello,\\nWorld!")'},
    # Special character: Tab character
    {"description": "Print with tab character", "code": 'print("Hello,\\tWorld!")'},
    # Special character: Unicode characters
    {"description": "Print with Unicode characters", "code": 'print("Hello, \\u2603 World!")'},
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Function to print all test data
def print_test_data(test_data):
    for data in test_data:
        print(f"Description: {data['description']}")
        print("Code:")
        print(data['code'])
        print("-" * 40)

# Print all test data
print_test_data(all_test_data)

