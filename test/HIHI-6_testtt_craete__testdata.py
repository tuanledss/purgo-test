import json
import xml.etree.ElementTree as ET
import csv
import random
import string

# Helper functions
def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def random_number(min_value=0, max_value=1000):
    return random.uniform(min_value, max_value)

# Test Data Categories

# Happy path test data (valid, expected scenarios)
happy_path_data = [
    # Valid JSON input
    {"id": random_string(), "value": random_number()} for _ in range(5)
]

# Edge case test data (boundary conditions)
edge_case_data = [
    # Minimum value
    {"id": random_string(), "value": 0},
    # Maximum value
    {"id": random_string(), "value": 1000},
    # Empty string ID
    {"id": "", "value": random_number()},
    # Long string ID
    {"id": random_string(256), "value": random_number()}
]

# Error case test data (invalid inputs)
error_case_data = [
    # Missing ID
    {"value": random_number()},
    # Missing value
    {"id": random_string()},
    # Null ID
    {"id": None, "value": random_number()},
    # Null value
    {"id": random_string(), "value": None},
    # Non-numeric value
    {"id": random_string(), "value": "non-numeric"}
]

# Special character and format test data
special_character_data = [
    # ID with special characters
    {"id": "!@#$%^&*()", "value": random_number()},
    # Value as a string with special characters
    {"id": random_string(), "value": "!@#$%^&*()"},
    # ID with spaces
    {"id": "   ", "value": random_number()},
    # Value as a string with spaces
    {"id": random_string(), "value": "   "}
]

# Data Generation Rules
all_test_data = happy_path_data + edge_case_data + error_case_data + special_character_data

# JSON to XML conversion
def json_to_xml(json_data):
    root = ET.Element("root")
    for item in json_data:
        data_elem = ET.SubElement(root, "data")
        id_elem = ET.SubElement(data_elem, "id")
        id_elem.text = str(item.get("id", ""))
        value_elem = ET.SubElement(data_elem, "value")
        value_elem.text = str(item.get("value", ""))
    return ET.tostring(root, encoding='unicode')

# XML to CSV conversion
def xml_to_csv(xml_data):
    root = ET.fromstring(xml_data)
    csv_data = []
    for data_elem in root.findall('data'):
        id_text = data_elem.find('id').text
        value_text = data_elem.find('value').text
        csv_data.append([id_text, value_text])
    return csv_data

# Generate and print test data
for i, test_case in enumerate(all_test_data):
    print(f"Test Case {i+1}:")
    print("JSON Input:", json.dumps(test_case))
    xml_output = json_to_xml([test_case])
    print("XML Output:", xml_output)
    csv_output = xml_to_csv(xml_output)
    print("CSV Output:", csv_output)
    print("-" * 50)

