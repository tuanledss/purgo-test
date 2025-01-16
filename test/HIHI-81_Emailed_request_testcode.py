import unittest
import requests
from unittest.mock import patch

class TestUserAPI(unittest.TestCase):

    BASE_URL = "http://localhost:5000/api/users"

    def setUp(self):
        # Setup code if needed
        pass

    def tearDown(self):
        # Teardown code if needed
        pass

    # Happy Path Tests
    def test_create_user_valid_data(self):
        """Test creating a user with valid data"""
        for data in happy_path_data:
            response = requests.post(self.BASE_URL, json=data)
            self.assertEqual(response.status_code, 201)
            self.assertEqual(response.json().get('email'), data['email'])

    # Edge Case Tests
    def test_create_user_edge_cases(self):
        """Test creating a user with edge case data"""
        for data in edge_case_data:
            response = requests.post(self.BASE_URL, json=data)
            self.assertEqual(response.status_code, 201)
            self.assertEqual(response.json().get('name'), data['name'])

    # Error Case Tests
    def test_create_user_invalid_email(self):
        """Test creating a user with invalid email format"""
        data = error_case_data[0]
        response = requests.post(self.BASE_URL, json=data)
        self.assertEqual(response.status_code, 400)
        self.assertIn('Invalid email format', response.text)

    def test_create_user_invalid_phone_number(self):
        """Test creating a user with invalid phone number format"""
        data = error_case_data[1]
        response = requests.post(self.BASE_URL, json=data)
        self.assertEqual(response.status_code, 400)
        self.assertIn('Invalid phone number format', response.text)

    def test_create_user_missing_fields(self):
        """Test creating a user with missing required fields"""
        data = error_case_data[2]
        response = requests.post(self.BASE_URL, json=data)
        self.assertEqual(response.status_code, 400)
        self.assertIn('Missing required fields', response.text)

    # Special Character Tests
    def test_create_user_special_characters(self):
        """Test creating a user with special characters in name and email"""
        for data in special_char_data:
            response = requests.post(self.BASE_URL, json=data)
            self.assertEqual(response.status_code, 201)
            self.assertEqual(response.json().get('name'), data['name'])

    # Exception Handling Tests
    @patch('requests.post')
    def test_create_user_server_error(self, mock_post):
        """Test server error handling when creating a user"""
        mock_post.side_effect = requests.exceptions.RequestException
        data = happy_path_data[0]
        with self.assertRaises(requests.exceptions.RequestException):
            requests.post(self.BASE_URL, json=data)

if __name__ == '__main__':
    unittest.main()
