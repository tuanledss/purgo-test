import random
import string
import json

# Helper functions
def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def random_email():
    return f"{random_string(5)}@{random_string(3)}.com"

def random_password():
    return random_string(12)

def random_transaction_id():
    return random_string(10)

def random_amount():
    return round(random.uniform(1.0, 1000.0), 2)

def random_currency():
    return random.choice(['USD', 'EUR', 'GBP'])

# Happy path test data
happy_path_data = [
    # Valid user data
    {"username": "user1", "email": "user1@example.com", "password": "Password123!"},
    {"username": "user2", "email": "user2@example.com", "password": "SecurePass456@"},
    # Valid transaction data
    {"transaction_id": "TXN1234567", "amount": 100.50, "currency": "USD"},
    {"transaction_id": "TXN7654321", "amount": 250.75, "currency": "EUR"},
]

# Edge case test data
edge_case_data = [
    # Boundary conditions for username length
    {"username": "u", "email": "short@example.com", "password": "ShortPass1!"},
    {"username": "u" * 50, "email": "long@example.com", "password": "LongPass123!"},
    # Boundary conditions for amount
    {"transaction_id": "TXN0000001", "amount": 0.01, "currency": "USD"},
    {"transaction_id": "TXN9999999", "amount": 999999.99, "currency": "GBP"},
]

# Error case test data
error_case_data = [
    # Invalid email format
    {"username": "user3", "email": "invalid-email", "password": "InvalidEmail1!"},
    # Missing required fields
    {"username": "user4", "email": "user4@example.com"},  # Missing password
    {"transaction_id": "TXN1234568", "amount": 100.50},  # Missing currency
]

# Special character and format test data
special_char_data = [
    # Special characters in username
    {"username": "user!@#", "email": "special@example.com", "password": "SpecialChar1!"},
    # Special characters in email
    {"username": "user5", "email": "user5@ex!ample.com", "password": "SpecialEmail1!"},
    # Special characters in transaction ID
    {"transaction_id": "TXN!@#123", "amount": 150.00, "currency": "USD"},
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_char_data

# Output test data as JSON
print(json.dumps(all_test_data, indent=2))


This code generates test data for a system with user and transaction data, covering happy path, edge cases, error cases, and special character scenarios. Each section is commented to explain the purpose of the test data, and the data is output in JSON format.