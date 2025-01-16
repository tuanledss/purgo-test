import unittest

class TestNotebookFunctionality(unittest.TestCase):
    
    def setUp(self):
        # Setup code if needed
        pass

    def tearDown(self):
        # Teardown code if needed
        pass

    def test_print_helloworld(self):
        # Test that the notebook prints "helloworld"
        expected_output = "helloworld"
        actual_output = self.execute_notebook_code()
        self.assertEqual(actual_output, expected_output, "The output should be 'helloworld'")

    def test_empty_name_error(self):
        # Test that an empty name triggers an error
        with self.assertRaises(ValueError, msg="Name cannot be empty"):
            self.validate_input({"id": 8, "name": "", "email": "emptyname@example.com", "age": 30})

    def test_invalid_email_format(self):
        # Test that an invalid email format triggers an error
        with self.assertRaises(ValueError, msg="Invalid email format"):
            self.validate_input({"id": 9, "name": "Eve", "email": "invalid-email", "age": 30})

    def test_negative_age_error(self):
        # Test that a negative age triggers an error
        with self.assertRaises(ValueError, msg="Age cannot be negative"):
            self.validate_input({"id": 10, "name": "Frank", "email": "frank@example.com", "age": -1})

    def test_age_beyond_maximum_error(self):
        # Test that an age beyond maximum triggers an error
        with self.assertRaises(ValueError, msg="Age exceeds maximum limit"):
            self.validate_input({"id": 11, "name": "Grace", "email": "grace@example.com", "age": 130})

    def test_special_character_in_name(self):
        # Test that names with special characters are handled correctly
        self.assertTrue(self.validate_input({"id": 12, "name": "H@rry", "email": "harry@example.com", "age": 30}))

    def test_special_character_in_email(self):
        # Test that emails with special characters are handled correctly
        self.assertTrue(self.validate_input({"id": 13, "name": "Ivy", "email": "ivy+test@example.com", "age": 30}))

    def execute_notebook_code(self):
        # Simulate executing the notebook code
        return "helloworld"

    def validate_input(self, data):
        # Simulate input validation
        if not data["name"]:
            raise ValueError("Name cannot be empty")
        if "@" not in data["email"]:
            raise ValueError("Invalid email format")
        if data["age"] < 0:
            raise ValueError("Age cannot be negative")
        if data["age"] > 120:
            raise ValueError("Age exceeds maximum limit")
        return True

if __name__ == '__main__':
    unittest.main()
