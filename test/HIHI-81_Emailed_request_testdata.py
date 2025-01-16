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

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid user data
    {"userId": 1, "name": "John Doe", "email": "john.doe@example.com", "phoneNumber": "+12345678901"},
    {"userId": 2, "name": "Jane Smith", "email": "jane.smith@example.com", "phoneNumber": "+19876543210"},
    # Valid transaction data
    {"transactionId": 101, "userId": 1, "amount": 100.50, "currency": "USD"},
    {"transactionId": 102, "userId": 2, "amount": 200.75, "currency": "EUR"},
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Minimum length for name
    {"userId": 3, "name": "A", "email": "a@example.com", "phoneNumber": "+12345678902"},
    # Maximum length for name
    {"userId": 4, "name": random_string(255), "email": "max.name@example.com", "phoneNumber": "+12345678903"},
    # Minimum valid transaction amount
    {"transactionId": 103, "userId": 3, "amount": 0.01, "currency": "USD"},
    # Maximum valid transaction amount
    {"transactionId": 104, "userId": 4, "amount": 1000000.00, "currency": "USD"},
]

# Error case test data (invalid inputs)
error_case_data = [
    # Invalid email format
    {"userId": 5, "name": "Invalid Email", "email": "invalid-email", "phoneNumber": "+12345678904"},
    # Invalid phone number format
    {"userId": 6, "name": "Invalid Phone", "email": "valid.email@example.com", "phoneNumber": "123456"},
    # Negative transaction amount
    {"transactionId": 105, "userId": 5, "amount": -50.00, "currency": "USD"},
    # Unsupported currency
    {"transactionId": 106, "userId": 6, "amount": 100.00, "currency": "XYZ"},
]

# Special character and format test data
special_character_data = [
    # Name with special characters
    {"userId": 7, "name": "John!@#$%^&*()", "email": "special.char@example.com", "phoneNumber": "+12345678905"},
    # Email with special characters
    {"userId": 8, "name": "Special Email", "email": "special!email@example.com", "phoneNumber": "+12345678906"},
    # Phone number with spaces
    {"userId": 9, "name": "Phone Space", "email": "phone.space@example.com", "phoneNumber": "+1 234 567 8907"},
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Output test data as JSON
print(json.dumps(all_test_data, indent=2))

