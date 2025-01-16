import random
import string
import hashlib

# Helper functions
def generate_username(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_password(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation, k=length))

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# Happy Path Test Data
# Valid usernames and passwords
happy_path_data = [
    {"username": generate_username(), "password": generate_password(10)},
    {"username": generate_username(), "password": generate_password(12)},
    {"username": generate_username(), "password": generate_password(15)},
]

# Edge Case Test Data
# Boundary conditions for password length
edge_case_data = [
    {"username": generate_username(), "password": generate_password(8)},  # Minimum valid length
    {"username": generate_username(), "password": generate_password(50)}, # Arbitrary long password
]

# Error Case Test Data
# Invalid inputs such as empty fields and short passwords
error_case_data = [
    {"username": "", "password": generate_password(10)},  # Empty username
    {"username": generate_username(), "password": ""},    # Empty password
    {"username": generate_username(), "password": "short"}, # Password too short
]

# Special Character and Format Test Data
# Usernames and passwords with special characters
special_character_data = [
    {"username": "user!@#", "password": generate_password(10)},
    {"username": generate_username(), "password": "P@ssw0rd!"}, # Common special character password
    {"username": "user123", "password": "12345678"}, # Numeric password
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Output the test data
for i, data in enumerate(all_test_data, start=1):
    print(f"Test Case {i}: Username: {data['username']}, Password: {data['password']}, Password Hash: {hash_password(data['password'])}")


This code generates test data for a user authentication system, covering happy path scenarios, edge cases, error cases, and special character scenarios. Each test case is printed with a hashed password to simulate storage in a database.