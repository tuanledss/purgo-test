import unittest
import json
from my_application import MyApplication  # Assuming the application code is in my_application.py

class TestMyApplication(unittest.TestCase):

    def setUp(self):
        # Initialize the application or any required setup before each test
        self.app = MyApplication()

    def tearDown(self):
        # Clean up resources or reset states after each test
        pass

    # Test cases for happy path scenarios
    def test_valid_user_data(self):
        # Test valid user data
        test_data = [
            {"username": "user1", "email": "user1@example.com", "password": "Password123!"},
            {"username": "user2", "email": "user2@example.com", "password": "SecurePass456@"}
        ]
        for data in test_data:
            response = self.app.create_user(data)
            self.assertEqual(response.status_code, 201)
            self.assertIn('user_id', response.json())

    def test_valid_transaction_data(self):
        # Test valid transaction data
        test_data = [
            {"transaction_id": "TXN1234567", "amount": 100.50, "currency": "USD"},
            {"transaction_id": "TXN7654321", "amount": 250.75, "currency": "EUR"}
        ]
        for data in test_data:
            response = self.app.create_transaction(data)
            self.assertEqual(response.status_code, 201)
            self.assertIn('transaction_id', response.json())

    # Test cases for edge scenarios
    def test_edge_case_username_length(self):
        # Test boundary conditions for username length
        test_data = [
            {"username": "u", "email": "short@example.com", "password": "ShortPass1!"},
            {"username": "u" * 50, "email": "long@example.com", "password": "LongPass123!"}
        ]
        for data in test_data:
            response = self.app.create_user(data)
            self.assertEqual(response.status_code, 201)
            self.assertIn('user_id', response.json())

    def test_edge_case_transaction_amount(self):
        # Test boundary conditions for transaction amount
        test_data = [
            {"transaction_id": "TXN0000001", "amount": 0.01, "currency": "USD"},
            {"transaction_id": "TXN9999999", "amount": 999999.99, "currency": "GBP"}
        ]
        for data in test_data:
            response = self.app.create_transaction(data)
            self.assertEqual(response.status_code, 201)
            self.assertIn('transaction_id', response.json())

    # Test cases for error scenarios
    def test_invalid_email_format(self):
        # Test invalid email format
        test_data = {"username": "user3", "email": "invalid-email", "password": "InvalidEmail1!"}
        response = self.app.create_user(test_data)
        self.assertEqual(response.status_code, 400)
        self.assertIn('error', response.json())

    def test_missing_required_fields(self):
        # Test missing required fields
        test_data = [
            {"username": "user4", "email": "user4@example.com"},  # Missing password
            {"transaction_id": "TXN1234568", "amount": 100.50}  # Missing currency
        ]
        for data in test_data:
            if 'password' not in data:
                response = self.app.create_user(data)
            else:
                response = self.app.create_transaction(data)
            self.assertEqual(response.status_code, 400)
            self.assertIn('error', response.json())

    # Test cases for special character scenarios
    def test_special_characters_in_username(self):
        # Test special characters in username
        test_data = {"username": "user!@#", "email": "special@example.com", "password": "SpecialChar1!"}
        response = self.app.create_user(test_data)
        self.assertEqual(response.status_code, 400)
        self.assertIn('error', response.json())

    def test_special_characters_in_email(self):
        # Test special characters in email
        test_data = {"username": "user5", "email": "user5@ex!ample.com", "password": "SpecialEmail1!"}
        response = self.app.create_user(test_data)
        self.assertEqual(response.status_code, 400)
        self.assertIn('error', response.json())

    def test_special_characters_in_transaction_id(self):
        # Test special characters in transaction ID
        test_data = {"transaction_id": "TXN!@#123", "amount": 150.00, "currency": "USD"}
        response = self.app.create_transaction(test_data)
        self.assertEqual(response.status_code, 400)
        self.assertIn('error', response.json())

if __name__ == '__main__':
    unittest.main()
