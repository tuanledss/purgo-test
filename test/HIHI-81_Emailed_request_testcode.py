import unittest
from my_module import MySystem  # Replace with actual module and class names

class TestMySystem(unittest.TestCase):

    def setUp(self):
        # Setup code to initialize the system or any dependencies
        self.system = MySystem()

    def tearDown(self):
        # Teardown code to clean up after tests
        pass

    # Happy Path Tests
    def test_valid_user_data_user123(self):
        # Test valid user data for user123
        data = {"username": "user123", "password": "Passw0rd!", "email": "user123@example.com"}
        result = self.system.process_user_data(data)
        self.assertTrue(result['success'])
        self.assertEqual(result['message'], "User data processed successfully")

    def test_valid_user_data_johnDoe(self):
        # Test valid user data for johnDoe
        data = {"username": "johnDoe", "password": "Secure1@", "email": "john.doe@example.com"}
        result = self.system.process_user_data(data)
        self.assertTrue(result['success'])
        self.assertEqual(result['message'], "User data processed successfully")

    def test_valid_user_data_alice2023(self):
        # Test valid user data for alice2023
        data = {"username": "alice2023", "password": "Alice@2023", "email": "alice@example.com"}
        result = self.system.process_user_data(data)
        self.assertTrue(result['success'])
        self.assertEqual(result['message'], "User data processed successfully")

    # Edge Case Tests
    def test_username_min_length(self):
        # Test username at minimum length
        data = {"username": "user1", "password": "Passw0rd!", "email": "user1@example.com"}
        result = self.system.process_user_data(data)
        self.assertTrue(result['success'])
        self.assertEqual(result['message'], "User data processed successfully")

    def test_username_max_length(self):
        # Test username at maximum length
        data = {"username": "user1234567890", "password": "Passw0rd!", "email": "user1234567890@example.com"}
        result = self.system.process_user_data(data)
        self.assertTrue(result['success'])
        self.assertEqual(result['message'], "User data processed successfully")

    def test_password_min_length(self):
        # Test password at minimum length
        data = {"username": "edgeUser", "password": "P@ssw0r", "email": "edge@example.com"}
        result = self.system.process_user_data(data)
        self.assertTrue(result['success'])
        self.assertEqual(result['message'], "User data processed successfully")

    # Error Case Tests
    def test_invalid_username_too_short(self):
        # Test invalid username too short
        data = {"username": "us", "password": "Passw0rd!", "email": "us@example.com"}
        result = self.system.process_user_data(data)
        self.assertFalse(result['success'])
        self.assertEqual(result['message'], "Username must be between 5 and 15 characters")

    def test_invalid_username_too_long(self):
        # Test invalid username too long
        data = {"username": "user12345678901", "password": "Passw0rd!", "email": "user12345678901@example.com"}
        result = self.system.process_user_data(data)
        self.assertFalse(result['success'])
        self.assertEqual(result['message'], "Username must be between 5 and 15 characters")

    def test_invalid_password_no_special_char(self):
        # Test invalid password with no special character or number
        data = {"username": "user123", "password": "password", "email": "user123@example.com"}
        result = self.system.process_user_data(data)
        self.assertFalse(result['success'])
        self.assertEqual(result['message'], "Password must contain at least one number and one special character")

    def test_invalid_password_no_letter(self):
        # Test invalid password with no letter
        data = {"username": "user123", "password": "12345678", "email": "user123@example.com"}
        result = self.system.process_user_data(data)
        self.assertFalse(result['success'])
        self.assertEqual(result['message'], "Password must contain at least one letter")

    def test_invalid_email_missing_at(self):
        # Test invalid email missing '@'
        data = {"username": "user123", "password": "Passw0rd!", "email": "user123example.com"}
        result = self.system.process_user_data(data)
        self.assertFalse(result['success'])
        self.assertEqual(result['message'], "Invalid email format")

    def test_invalid_email_missing_domain(self):
        # Test invalid email missing domain
        data = {"username": "user123", "password": "Passw0rd!", "email": "user123@.com"}
        result = self.system.process_user_data(data)
        self.assertFalse(result['success'])
        self.assertEqual(result['message'], "Invalid email format")

    # Special Character Tests
    def test_special_characters_in_username(self):
        # Test special characters in username
        data = {"username": "user!@#", "password": "Passw0rd!", "email": "user!@#@example.com"}
        result = self.system.process_user_data(data)
        self.assertFalse(result['success'])
        self.assertEqual(result['message'], "Username must be alphanumeric")

    def test_special_characters_in_email(self):
        # Test special characters in email
        data = {"username": "user123", "password": "Passw0rd!", "email": "user123@ex!ample.com"}
        result = self.system.process_user_data(data)
        self.assertFalse(result['success'])
        self.assertEqual(result['message'], "Invalid email format")

    def test_special_characters_in_password(self):
        # Test special characters in password
        data = {"username": "user123", "password": "P@ssw0rd!@#", "email": "user123@example.com"}
        result = self.system.process_user_data(data)
        self.assertTrue(result['success'])
        self.assertEqual(result['message'], "User data processed successfully")

if __name__ == '__main__':
    unittest.main()
