import random
import string
from datetime import datetime, timedelta

# Helper functions
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def random_address():
    return {
        "address1": random_string(15),
        "state": random_string(2).upper(),
        "city": random_string(10),
        "postal_code": ''.join(random.choices(string.digits, k=5))
    }

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid agreement and customer data
    {
        "agree_no": random_string(8),
        "agree_desc": "Standard Agreement",
        "agree_type": "TypeA",
        "material_number": random_string(5),
        "master_agreement_type": "MasterType1",
        "source_customer_id": "CUST001",
        **random_address()
    },
    # Valid deal and customer data
    {
        "agree_no": random_string(8),
        "agree_desc": "Premium Agreement",
        "agree_type": "TypeB",
        "material_number": random_string(5),
        "master_agreement_type": "MasterType2",
        "source_customer_id": "CUST002",
        **random_address()
    }
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Minimum length strings
    {
        "agree_no": "A",
        "agree_desc": "A",
        "agree_type": "A",
        "material_number": "A",
        "master_agreement_type": "A",
        "source_customer_id": "CUST003",
        **random_address()
    },
    # Maximum length strings
    {
        "agree_no": random_string(50),
        "agree_desc": random_string(255),
        "agree_type": random_string(50),
        "material_number": random_string(50),
        "master_agreement_type": random_string(50),
        "source_customer_id": "CUST004",
        **random_address()
    }
]

# Error case test data (invalid inputs)
error_case_data = [
    # Invalid source_customer_id
    {
        "agree_no": random_string(8),
        "agree_desc": "Invalid Customer ID",
        "agree_type": "TypeC",
        "material_number": random_string(5),
        "master_agreement_type": "MasterType3",
        "source_customer_id": None,  # Invalid
        **random_address()
    },
    # Invalid agree_no format
    {
        "agree_no": "12345678",  # Should be alphanumeric
        "agree_desc": "Invalid Agree No",
        "agree_type": "TypeD",
        "material_number": random_string(5),
        "master_agreement_type": "MasterType4",
        "source_customer_id": "CUST005",
        **random_address()
    }
]

# Special character and format test data
special_character_data = [
    # Special characters in agree_desc
    {
        "agree_no": random_string(8),
        "agree_desc": "Special!@#$%^&*()",
        "agree_type": "TypeE",
        "material_number": random_string(5),
        "master_agreement_type": "MasterType5",
        "source_customer_id": "CUST006",
        **random_address()
    },
    # Special characters in address
    {
        "agree_no": random_string(8),
        "agree_desc": "Special Address",
        "agree_type": "TypeF",
        "material_number": random_string(5),
        "master_agreement_type": "MasterType6",
        "source_customer_id": "CUST007",
        "address1": "123 Main St!@#",
        "state": "CA",
        "city": "Los Angeles",
        "postal_code": "90001"
    }
]

# Combine all test data
test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# Output the test data
for record in test_data:
    print(record)
