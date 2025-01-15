import json
import random
import string
import re

# Helper functions
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_email():
    return f"{random_string(5)}@{random_string(3)}.com"

def random_age():
    return random.randint(1, 120)

def is_valid_email(email):
    return re.match(r"[^@]+@[^@]+\.[^@]+", email)

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid user data
    {"name": "Alice", "age": 30, "email": "alice@example.com"},
    {"name": "Bob", "age": 25, "email": "bob@example.com"},
    {"name": "Charlie", "age": 40, "email": "charlie@example.com"},
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Minimum age
    {"name": "Young", "age": 1, "email": "young@example.com"},
    # Maximum age
    {"name": "Old", "age": 120, "email": "old@example.com"},
    # Minimum name length
    {"name": "A", "age": 20, "email": "a@example.com"},
    # Maximum name length
    {"name": random_string(100), "age": 35, "email": "longname@example.com"},
]

# Error case test data (invalid inputs)
error_case_data = [
    # Invalid email format
    {"name": "InvalidEmail", "age": 30, "email": "invalidemail.com"},
    # Negative age
    {"name": "NegativeAge", "age": -5, "email": "negative@example.com"},
    # Age as string
    {"name": "StringAge", "age": "twenty", "email": "stringage@example.com"},
    # Missing name
    {"age": 30, "email": "noname@example.com"},
    # Missing age
    {"name": "NoAge", "email": "noage@example.com"},
    # Missing email
    {"name": "NoEmail", "age": 30},
]

# Special character and format test data
special_character_data = [
    # Name with special characters
    {"name": "Special!@#", "age": 30, "email": "special@example.com"},
    # Email with subdomain
    {"name": "Subdomain", "age": 30, "email": "sub@domain.example.com"},
    # Email with plus sign
    {"name": "PlusSign", "age": 30, "email": "plus+sign@example.com"},
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Validate and print test data
for record in all_test_data:
    # Validate data types and formats
    if 'name' in record and not isinstance(record['name'], str):
        print(f"Invalid name format: {record}")
    if 'age' in record and not isinstance(record['age'], int):
        print(f"Invalid age format: {record}")
    if 'email' in record and not is_valid_email(record.get('email', '')):
        print(f"Invalid email format: {record}")

    # Print valid test data
    print(json.dumps(record, indent=2))

