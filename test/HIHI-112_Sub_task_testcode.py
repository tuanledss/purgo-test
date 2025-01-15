import unittest
import json
from unittest.mock import patch

# Assuming the backend service is implemented in a module named `backend_service`
# and it has a function `process_user_data` that takes a user data dictionary as input
# and returns a response dictionary with 'status' and 'message' keys.

# Mock function to simulate backend service processing
def mock_process_user_data(user_data):
    if not user_data['userId']:
        return {'status': 400, 'message': 'User ID cannot be empty'}
    if not is_valid_email(user_data['email']):
        return {'status': 400, 'message': 'Invalid email format'}
    if user_data['role'] not in ['admin', 'user', 'guest']:
        return {'status': 400, 'message': 'Unsupported role'}
    return {'status': 200, 'message': 'User data processed successfully'}

class TestUserDataProcessing(unittest.TestCase):

    def setUp(self):
        # Setup code if needed
        pass

    def tearDown(self):
        # Teardown code if needed
        pass

    # Happy path test cases
    def test_valid_user_data(self):
        # Test valid user data scenarios
        for data in happy_path_data:
            with self.subTest(data=data):
                response = mock_process_user_data(data)
                self.assertEqual(response['status'], 200)
                self.assertEqual(response['message'], 'User data processed successfully')

    # Edge case test cases
    def test_edge_case_user_data(self):
        # Test edge case scenarios
        for data in edge_case_data:
            with self.subTest(data=data):
                response = mock_process_user_data(data)
                self.assertEqual(response['status'], 200)
                self.assertEqual(response['message'], 'User data processed successfully')

    # Error case test cases
    def test_error_case_user_data(self):
        # Test error scenarios
        for data in error_case_data:
            with self.subTest(data=data):
                response = mock_process_user_data(data)
                self.assertEqual(response['status'], 400)

    # Special character test cases
    def test_special_character_user_data(self):
        # Test special character scenarios
        for data in special_character_data:
            with self.subTest(data=data):
                response = mock_process_user_data(data)
                self.assertEqual(response['status'], 200)
                self.assertEqual(response['message'], 'User data processed successfully')

    # Test invalid email format
    def test_invalid_email_format(self):
        data = {"userId": "invalidEmail", "name": "Invalid Email", "email": "invalid-email", "role": "admin"}
        response = mock_process_user_data(data)
        self.assertEqual(response['status'], 400)
        self.assertEqual(response['message'], 'Invalid email format')

    # Test empty userId
    def test_empty_user_id(self):
        data = {"userId": "", "name": "Empty UserId", "email": "empty.userid@example.com", "role": "user"}
        response = mock_process_user_data(data)
        self.assertEqual(response['status'], 400)
        self.assertEqual(response['message'], 'User ID cannot be empty')

    # Test unsupported role
    def test_unsupported_role(self):
        data = {"userId": "unsupportedRole", "name": "Unsupported Role", "email": "unsupported.role@example.com", "role": "superuser"}
        response = mock_process_user_data(data)
        self.assertEqual(response['status'], 400)
        self.assertEqual(response['message'], 'Unsupported role')

if __name__ == '__main__':
    unittest.main()
