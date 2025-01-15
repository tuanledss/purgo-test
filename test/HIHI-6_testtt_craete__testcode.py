import unittest
import json
from my_application import app  # Assuming the Flask app is named 'app' in 'my_application.py'

class TestUserAPI(unittest.TestCase):

    def setUp(self):
        # Set up test client
        self.client = app.test_client()
        self.client.testing = True

    def test_happy_path_valid_user_data(self):
        # Test valid user data
        happy_path_data = [
            {"name": "Alice", "age": 30, "email": "alice@example.com"},
            {"name": "Bob", "age": 25, "email": "bob@example.com"},
            {"name": "Charlie", "age": 40, "email": "charlie@example.com"},
        ]
        for data in happy_path_data:
            response = self.client.post('/api/v1/resource', data=json.dumps(data), content_type='application/json')
            self.assertEqual(response.status_code, 200)
            self.assertIn('success', response.json)

    def test_edge_case_boundary_conditions(self):
        # Test edge cases for boundary conditions
        edge_case_data = [
            {"name": "Young", "age": 1, "email": "young@example.com"},
            {"name": "Old", "age": 120, "email": "old@example.com"},
            {"name": "A", "age": 20, "email": "a@example.com"},
            {"name": "LongName", "age": 35, "email": "longname@example.com"},
        ]
        for data in edge_case_data:
            response = self.client.post('/api/v1/resource', data=json.dumps(data), content_type='application/json')
            self.assertEqual(response.status_code, 200)
            self.assertIn('success', response.json)

    def test_error_case_invalid_inputs(self):
        # Test invalid inputs
        error_case_data = [
            {"name": "InvalidEmail", "age": 30, "email": "invalidemail.com"},
            {"name": "NegativeAge", "age": -5, "email": "negative@example.com"},
            {"name": "StringAge", "age": "twenty", "email": "stringage@example.com"},
            {"age": 30, "email": "noname@example.com"},
            {"name": "NoAge", "email": "noage@example.com"},
            {"name": "NoEmail", "age": 30},
        ]
        for data in error_case_data:
            response = self.client.post('/api/v1/resource', data=json.dumps(data), content_type='application/json')
            self.assertEqual(response.status_code, 400)
            self.assertIn('error', response.json)

    def test_special_character_and_format(self):
        # Test special characters and email formats
        special_character_data = [
            {"name": "Special!@#", "age": 30, "email": "special@example.com"},
            {"name": "Subdomain", "age": 30, "email": "sub@domain.example.com"},
            {"name": "PlusSign", "age": 30, "email": "plus+sign@example.com"},
        ]
        for data in special_character_data:
            response = self.client.post('/api/v1/resource', data=json.dumps(data), content_type='application/json')
            self.assertEqual(response.status_code, 200)
            self.assertIn('success', response.json)

    def tearDown(self):
        # Clean up after each test
        pass

if __name__ == '__main__':
    unittest.main()
