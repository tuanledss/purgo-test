import unittest
import uuid
from my_module import process_request  # Assuming the function to test is named process_request

class TestProcessRequest(unittest.TestCase):

    def setUp(self):
        # Setup code if needed
        pass

    def tearDown(self):
        # Teardown code if needed
        pass

    # Helper functions
    def generate_uuid(self):
        return str(uuid.uuid4())

    def test_happy_path_valid_userId_and_action(self):
        # Test valid userId and action
        test_data = [
            {"userId": self.generate_uuid(), "action": "fetchData"},
            {"userId": self.generate_uuid(), "action": "updateData"},
            {"userId": self.generate_uuid(), "action": "deleteData"},
            {"userId": self.generate_uuid(), "action": "createData"},
            {"userId": self.generate_uuid(), "action": "FetchData"},
            {"userId": self.generate_uuid(), "action": "UPDATEDATA"},
        ]
        for data in test_data:
            with self.subTest(data=data):
                response = process_request(data)
                self.assertEqual(response['status'], 'success')

    def test_edge_case_minimum_length_userId(self):
        # Test minimum length userId (UUID length)
        data = {"userId": self.generate_uuid(), "action": "fetchData"}
        response = process_request(data)
        self.assertEqual(response['status'], 'success')

    def test_edge_case_maximum_length_action(self):
        # Test maximum length action
        data = {"userId": self.generate_uuid(), "action": "a" * 20}
        response = process_request(data)
        self.assertEqual(response['status'], 'success')

    def test_error_case_invalid_userId_format(self):
        # Test invalid userId format
        test_data = [
            {"userId": "12345", "action": "fetchData"},
            {"userId": "invalid-uuid", "action": "updateData"},
        ]
        for data in test_data:
            with self.subTest(data=data):
                response = process_request(data)
                self.assertEqual(response['status'], 'error')
                self.assertIn('Invalid userId format', response['message'])

    def test_error_case_invalid_action(self):
        # Test invalid action
        data = {"userId": self.generate_uuid(), "action": "invalidAction"}
        response = process_request(data)
        self.assertEqual(response['status'], 'error')
        self.assertIn('Invalid action', response['message'])

    def test_error_case_missing_userId(self):
        # Test missing userId
        data = {"action": "fetchData"}
        response = process_request(data)
        self.assertEqual(response['status'], 'error')
        self.assertIn('Missing userId', response['message'])

    def test_error_case_missing_action(self):
        # Test missing action
        data = {"userId": self.generate_uuid()}
        response = process_request(data)
        self.assertEqual(response['status'], 'error')
        self.assertIn('Missing action', response['message'])

    def test_special_character_in_action(self):
        # Test special characters in action
        test_data = [
            {"userId": self.generate_uuid(), "action": "fetch@Data"},
            {"userId": self.generate_uuid(), "action": "update#Data"},
        ]
        for data in test_data:
            with self.subTest(data=data):
                response = process_request(data)
                self.assertEqual(response['status'], 'error')
                self.assertIn('Invalid action', response['message'])

    def test_special_character_in_userId(self):
        # Test special characters in userId
        data = {"userId": "123e4567-e89b-12d3-a456-426614174000$", "action": "fetchData"}
        response = process_request(data)
        self.assertEqual(response['status'], 'error')
        self.assertIn('Invalid userId format', response['message'])

    def test_json_with_special_characters(self):
        # Test JSON with special characters
        data = {"userId": self.generate_uuid(), "action": "fetchData", "extra": "!@#$%^&*()"}
        response = process_request(data)
        self.assertEqual(response['status'], 'success')

if __name__ == '__main__':
    unittest.main()
