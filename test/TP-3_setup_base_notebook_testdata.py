# Import necessary libraries
import random
import string

# Happy Path Test Data
# Test case: Valid scenario where the notebook prints "Hello, World!"
happy_path_1 = {
    "platform": "Jupyter Notebook",
    "language": "Python",
    "version": "3.8",
    "output": "Hello, World!"
}

# Edge Case Test Data
# Test case: Boundary condition with minimum valid Python version
edge_case_1 = {
    "platform": "Jupyter Notebook",
    "language": "Python",
    "version": "3.6",  # Assuming 3.6 is the minimum supported version
    "output": "Hello, World!"
}

# Test case: Boundary condition with maximum valid Python version
edge_case_2 = {
    "platform": "Jupyter Notebook",
    "language": "Python",
    "version": "3.9",  # Assuming 3.9 is the maximum supported version
    "output": "Hello, World!"
}

# Error Case Test Data
# Test case: Invalid platform
error_case_1 = {
    "platform": "Google Colab",  # Invalid as per requirements
    "language": "Python",
    "version": "3.8",
    "output": None  # Expecting no output due to invalid platform
}

# Test case: Unsupported Python version
error_case_2 = {
    "platform": "Jupyter Notebook",
    "language": "Python",
    "version": "2.7",  # Unsupported version
    "output": None  # Expecting no output due to unsupported version
}

# Special Character and Format Test Data
# Test case: Special characters in output
special_char_1 = {
    "platform": "Jupyter Notebook",
    "language": "Python",
    "version": "3.8",
    "output": "Hello, World! 😊"  # Special character in output
}

# Test case: Different text format
special_format_1 = {
    "platform": "Jupyter Notebook",
    "language": "Python",
    "version": "3.8",
    "output": "HELLO, WORLD!"  # Uppercase format
}

# Generate additional test data
def generate_test_data():
    test_data = []

    # Generate random valid test data
    for _ in range(5):
        test_data.append({
            "platform": "Jupyter Notebook",
            "language": "Python",
            "version": random.choice(["3.6", "3.7", "3.8", "3.9"]),
            "output": "Hello, World!"
        })

    # Generate random invalid test data
    for _ in range(5):
        test_data.append({
            "platform": random.choice(["Google Colab", "VS Code"]),
            "language": "Python",
            "version": random.choice(["2.7", "3.5"]),
            "output": None
        })

    return test_data

# Combine all test data
all_test_data = [
    happy_path_1,
    edge_case_1,
    edge_case_2,
    error_case_1,
    error_case_2,
    special_char_1,
    special_format_1
] + generate_test_data()

# Print all test data
for i, data in enumerate(all_test_data, start=1):
    print(f"Test Data {i}: {data}")
