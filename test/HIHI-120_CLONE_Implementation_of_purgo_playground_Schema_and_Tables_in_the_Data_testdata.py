import random
import string
from datetime import datetime, timedelta

# Helper functions
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def random_country_code():
    return random.choice(['US', 'CA', 'GB', 'FR', 'DE', 'JP', 'CN', 'IN', 'BR', 'AU'])

# Test Data Generation

# Happy path test data
happy_path_data = [
    # Valid program data
    {
        "program_id": 1,
        "program_name": "Health Program",
        "country_code": "US",
        "program_start_date": datetime(2023, 1, 1)
    },
    {
        "program_id": 2,
        "program_name": "Education Program",
        "country_code": "CA",
        "program_start_date": datetime(2023, 2, 1)
    },
    # Valid interaction data
    {
        "interaction_id": 1,
        "enrollment_id": 1,
        "interaction_date": datetime(2023, 3, 1)
    },
    {
        "interaction_id": 2,
        "enrollment_id": 2,
        "interaction_date": datetime(2023, 4, 1)
    }
]

# Edge case test data
edge_case_data = [
    # Boundary program data
    {
        "program_id": 0,
        "program_name": "Boundary Program",
        "country_code": "GB",
        "program_start_date": datetime(1970, 1, 1)
    },
    {
        "program_id": 999999999999,
        "program_name": "Max ID Program",
        "country_code": "FR",
        "program_start_date": datetime(9999, 12, 31)
    },
    # Boundary interaction data
    {
        "interaction_id": 0,
        "enrollment_id": 0,
        "interaction_date": datetime(1970, 1, 1)
    },
    {
        "interaction_id": 999999999999,
        "enrollment_id": 999999999999,
        "interaction_date": datetime(9999, 12, 31)
    }
]

# Error case test data
error_case_data = [
    # Invalid program data
    {
        "program_id": -1,
        "program_name": "",
        "country_code": "ZZ",
        "program_start_date": None
    },
    {
        "program_id": "abc",
        "program_name": 123,
        "country_code": None,
        "program_start_date": "not a date"
    },
    # Invalid interaction data
    {
        "interaction_id": -1,
        "enrollment_id": -1,
        "interaction_date": "not a date"
    },
    {
        "interaction_id": "abc",
        "enrollment_id": "xyz",
        "interaction_date": None
    }
]

# Special character and format test data
special_char_data = [
    # Program data with special characters
    {
        "program_id": 3,
        "program_name": "Special!@#$%^&*()_+",
        "country_code": "DE",
        "program_start_date": datetime(2023, 5, 1)
    },
    {
        "program_id": 4,
        "program_name": "Format Test",
        "country_code": "JP",
        "program_start_date": datetime(2023, 6, 1)
    },
    # Interaction data with special characters
    {
        "interaction_id": 3,
        "enrollment_id": 3,
        "interaction_date": datetime(2023, 7, 1)
    },
    {
        "interaction_id": 4,
        "enrollment_id": 4,
        "interaction_date": datetime(2023, 8, 1)
    }
]

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_char_data

# Print the generated test data
for data in all_test_data:
    print(data)
