import unittest
import json
from my_application import MyApplication  # Assuming the application code is in my_application.py
from testdata import happy_path_data, edge_case_data, error_case_data, special_character_data

class TestMyApplication(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Setup that runs once before all tests
        cls.app = MyApplication()

    def setUp(self):
        # Setup that runs before each test
        self.client = self.app.test_client()

    def tearDown(self):
        # Teardown that runs after each test
        pass

    @classmethod
    def tearDownClass(cls):
        # Teardown that runs once after all tests
        pass

    # Test happy path scenarios
    def test_happy_path_scenarios(self):
        for data in happy_path_data:
            with self.subTest(data=data):
                response = self.client.post('/api/register', json=data)
                self.assertEqual(response.status_code, 201)
                self.assertIn('user_id', response.json)

    # Test edge case scenarios
    def test_edge_case_scenarios(self):
        for data in edge_case_data:
            with self.subTest(data=data):
                response = self.client.post('/api/register', json=data)
                self.assertEqual(response.status_code, 201)
                self.assertIn('user_id', response.json)

    # Test error case scenarios
    def test_error_case_scenarios(self):
        for data in error_case_data:
            with self.subTest(data=data):
                response = self.client.post('/api/register', json=data)
                self.assertEqual(response.status_code, 400)
                self.assertIn('error', response.json)

    # Test special character scenarios
    def test_special_character_scenarios(self):
        for data in special_character_data:
            with self.subTest(data=data):
                response = self.client.post('/api/register', json=data)
                self.assertEqual(response.status_code, 201)
                self.assertIn('user_id', response.json)

    # Test invalid email format
    def test_invalid_email_format(self):
        data = {
            "username": "testuser",
            "password": "testpassword",
            "email": "invalid-email",
            "phone": "1234567890"
        }
        response = self.client.post('/api/register', json=data)
        self.assertEqual(response.status_code, 400)
        self.assertIn('error', response.json)

    # Test missing username
    def test_missing_username(self):
        data = {
            "username": "",
            "password": "testpassword",
            "email": "test@example.com",
            "phone": "1234567890"
        }
        response = self.client.post('/api/register', json=data)
        self.assertEqual(response.status_code, 400)
        self.assertIn('error', response.json)

    # Test missing password
    def test_missing_password(self):
        data = {
            "username": "testuser",
            "password": "",
            "email": "test@example.com",
            "phone": "1234567890"
        }
        response = self.client.post('/api/register', json=data)
        self.assertEqual(response.status_code, 400)
        self.assertIn('error', response.json)

    # Test invalid phone number
    def test_invalid_phone_number(self):
        data = {
            "username": "testuser",
            "password": "testpassword",
            "email": "test@example.com",
            "phone": "123"
        }
        response = self.client.post('/api/register', json=data)
        self.assertEqual(response.status_code, 400)
        self.assertIn('error', response.json)

if __name__ == '__main__':
    unittest.main()
