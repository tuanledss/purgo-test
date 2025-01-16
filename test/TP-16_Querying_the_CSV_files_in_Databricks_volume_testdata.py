import random
import string
import datetime

# Happy Path Test Data
# Valid scenarios with expected inputs
happy_path_data = [
    {
        "product_id": "P001",
        "revenue": 1000.00,
        "date": "2023-01-01"
    },
    {
        "product_id": "P002",
        "revenue": 1500.50,
        "date": "2023-02-15"
    },
    {
        "product_id": "P003",
        "revenue": 2000.75,
        "date": "2023-03-20"
    }
]

# Edge Case Test Data
# Boundary conditions such as minimum and maximum values
edge_case_data = [
    {
        "product_id": "P004",
        "revenue": 0.00,  # Minimum revenue
        "date": "2023-01-01"
    },
    {
        "product_id": "P005",
        "revenue": 9999999.99,  # Maximum revenue
        "date": "2023-12-31"
    }
]

# Error Case Test Data
# Invalid inputs to test error handling
error_case_data = [
    {
        "product_id": "",  # Empty product_id
        "revenue": 500.00,
        "date": "2023-04-01"
    },
    {
        "product_id": "P006",
        "revenue": -100.00,  # Negative revenue
        "date": "2023-05-01"
    },
    {
        "product_id": "P007",
        "revenue": 100.00,
        "date": "2023-02-30"  # Invalid date
    }
]

# Special Character and Format Test Data
# Inputs with special characters and different formats
special_character_data = [
    {
        "product_id": "P@008",
        "revenue": 750.00,
        "date": "2023-06-01"
    },
    {
        "product_id": "P009",
        "revenue": 1250.00,
        "date": "01-07-2023"  # Different date format
    },
    {
        "product_id": "P010",
        "revenue": 500.00,
        "date": "2023/08/01"  # Different date separator
    }
]

# Function to generate random test data
def generate_random_test_data(num_records):
    random_data = []
    for _ in range(num_records):
        product_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
        revenue = round(random.uniform(0, 10000), 2)
        date = datetime.date.today() - datetime.timedelta(days=random.randint(0, 365))
        random_data.append({
            "product_id": product_id,
            "revenue": revenue,
            "date": date.strftime("%Y-%m-%d")
        })
    return random_data

# Generate additional random test data
random_test_data = generate_random_test_data(10)

# Combine all test data
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data + random_test_data

# Print all test data
for record in all_test_data:
    print(record)
