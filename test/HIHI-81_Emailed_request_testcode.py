import unittest
import json
from my_application import process_user_data, process_transaction_data, validate_user_data, validate_transaction_data

class TestUserDataProcessing(unittest.TestCase):

    def setUp(self):
        # Load test data
        with open('test_data.json') as f:
            self.test_data = json.load(f)

    def test_happy_path_user_data(self):
        # Test valid user data processing
        for data in self.test_data['happy_path_data']:
            if 'userId' in data:
                result = process_user_data(data)
                self.assertTrue(result['success'])
                self.assertEqual(result['userId'], data['userId'])

    def test_edge_case_user_data(self):
        # Test edge cases for user data
        for data in self.test_data['edge_case_data']:
            if 'userId' in data:
                result = process_user_data(data)
                self.assertTrue(result['success'])
                self.assertEqual(result['userId'], data['userId'])

    def test_error_case_user_data(self):
        # Test invalid user data processing
        for data in self.test_data['error_case_data']:
            if 'userId' in data:
                result = validate_user_data(data)
                self.assertFalse(result['success'])
                self.assertIn('error', result)

    def test_special_character_user_data(self):
        # Test user data with special characters
        for data in self.test_data['special_character_data']:
            if 'userId' in data:
                result = process_user_data(data)
                self.assertTrue(result['success'])
                self.assertEqual(result['userId'], data['userId'])

class TestTransactionDataProcessing(unittest.TestCase):

    def setUp(self):
        # Load test data
        with open('test_data.json') as f:
            self.test_data = json.load(f)

    def test_happy_path_transaction_data(self):
        # Test valid transaction data processing
        for data in self.test_data['happy_path_data']:
            if 'transactionId' in data:
                result = process_transaction_data(data)
                self.assertTrue(result['success'])
                self.assertEqual(result['transactionId'], data['transactionId'])

    def test_edge_case_transaction_data(self):
        # Test edge cases for transaction data
        for data in self.test_data['edge_case_data']:
            if 'transactionId' in data:
                result = process_transaction_data(data)
                self.assertTrue(result['success'])
                self.assertEqual(result['transactionId'], data['transactionId'])

    def test_error_case_transaction_data(self):
        # Test invalid transaction data processing
        for data in self.test_data['error_case_data']:
            if 'transactionId' in data:
                result = validate_transaction_data(data)
                self.assertFalse(result['success'])
                self.assertIn('error', result)

    def test_special_character_transaction_data(self):
        # Test transaction data with special characters
        for data in self.test_data['special_character_data']:
            if 'transactionId' in data:
                result = process_transaction_data(data)
                self.assertTrue(result['success'])
                self.assertEqual(result['transactionId'], data['transactionId'])

if __name__ == '__main__':
    unittest.main()
