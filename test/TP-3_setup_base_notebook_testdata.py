# Import necessary libraries
import random
import string

# Happy Path Test Data
# These are valid scenarios where the notebook should print "Hello, World!" correctly.

# Test Case 1: Basic valid input
happy_path_1 = "print('Hello, World!')"  # Expected output: Hello, World!

# Edge Case Test Data
# These test the boundary conditions of the input.

# Test Case 2: Minimal valid input with different quotes
edge_case_1 = 'print("Hello, World!")'  # Expected output: Hello, World!

# Test Case 3: Input with leading and trailing spaces
edge_case_2 = "   print('Hello, World!')   "  # Expected output: Hello, World!

# Error Case Test Data
# These are invalid inputs that should be handled gracefully.

# Test Case 4: Missing parentheses
error_case_1 = "print 'Hello, World!'"  # Expected output: SyntaxError

# Test Case 5: Misspelled print function
error_case_2 = "prnt('Hello, World!')"  # Expected output: NameError

# Test Case 6: Incorrect string delimiter
error_case_3 = "print('Hello, World!)"  # Expected output: SyntaxError

# Special Character and Format Test Data
# These test the handling of special characters and formats.

# Test Case 7: Input with escape characters
special_char_1 = "print('Hello,\\nWorld!')"  # Expected output: Hello, (newline) World!

# Test Case 8: Input with Unicode characters
special_char_2 = "print('Hello, World! 😊')"  # Expected output: Hello, World! 😊

# Test Case 9: Input with special characters
special_char_3 = "print('Hello, World! @#$%^&*()')"  # Expected output: Hello, World! @#$%^&*()

# Generate a list of all test cases
test_cases = [
    happy_path_1,
    edge_case_1,
    edge_case_2,
    error_case_1,
    error_case_2,
    error_case_3,
    special_char_1,
    special_char_2,
    special_char_3
]

# Function to execute and print the result of each test case
def execute_test_cases(test_cases):
    for i, test_case in enumerate(test_cases, start=1):
        print(f"Executing Test Case {i}: {test_case}")
        try:
            exec(test_case)
        except Exception as e:
            print(f"Error: {e}")

# Execute all test cases
execute_test_cases(test_cases)

