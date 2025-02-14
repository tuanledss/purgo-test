from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime

spark = SparkSession.builder.appName("TestDataGeneration").getOrCreate()

# Define schema with Databricks data types
schema = StructType([
    StructField("id", BigintType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("company", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("account_manager", StringType(), True),
    StructField("creation_date", DateType(), True),
    StructField("last_interaction_date", DateType(), True),
    StructField("purchase_history", StringType(), True), # Example: JSON string, ArrayType, etc.
    StructField("notes", StringType(), True),
    StructField("zip", StringType(), True)
])

# Happy Path
data_happy_path = [
    (1, "John Doe", "john.doe@example.com", "123-456-7890", "Acme Corp", "Software Engineer", "123 Main St", "Anytown", "CA", "USA", "Technology", "Alice Smith", datetime.date(2023, 1, 15), datetime.date(2024, 3, 10), "[{\"product\": \"A\", \"amount\": 100}, {\"product\": \"B\", \"amount\": 200}]", "Regular customer", "90210"),
    (2, "Jane Doe", "jane.doe@example.com", "+1-987-654-3210", "Globex Corp", "Data Scientist", "456 Oak Ave", "Springfield", "IL", "USA", "Finance", "Bob Johnson", datetime.date(2023, 5, 20), datetime.date(2024, 3, 15), "[{\"product\": \"C\", \"amount\": 150}]", "New customer", "60601"),
    # ... more happy path records (up to 20-30 total)
]

# Edge Cases
data_edge_cases = [
    (3, "A very long name that exceeds the typical length", "valid.email@example.com", "1-111-111-1111", "Edge Case Corp", "Test Engineer", "1 Edge Case Ln", "Edge City", "ZZ", "USA", "Testing", "Charles Brown", datetime.date(2000, 1, 1), datetime.date(2024, 3, 20), "[]", "First customer", "00000"),  # Long name, earliest date
    (4, "User4", "user4@example.com", "555-123-4567", "", "", "", "", "", "", "", "", datetime.date(2024, 3, 21), datetime.date(9999, 12, 31), "{}", "", "99999"), # Empty fields, latest date
    # ...more edge case records
]

# Error Cases (using valid data types, representing invalid input scenarios)
data_error_cases = [
    (5, "", "invalid_email", "123", "Invalid Input Corp", "Error Creator", "Invalid Address", "Error City", "", "Invalid Country", "Invalid Industry", "David Lee", None, None, None, "Invalid Notes", "1234"), # Invalid email, phone, zip, potentially other fields
    # ... more error case records
]

# NULL handling scenarios
data_null_handling = [
    (6, None, "user_null@example.com", None, None, None, None, None, None, None, None, None, None, None, None, None, None), # All nullable fields are NULL
    (7, "User with some NULLs", "somenulls@example.com", "555-555-5555", None, "Some Job", None, None, None, None, None, None, datetime.date(2023, 12, 31), None, None, None, "55555"), # Some fields NULL
    #... more NULL handling records
]

# Special characters and multi-byte characters
data_special_chars = [
    (8, "User with special chars: !@#$%^&*()", "special@chars.com", "555-555-5555", "Special Chars Inc.", "Special Char Specialist", "1 Special Char St.", "Special City", "SC", "USA", "Special Char Handling", "Eve Jackson", datetime.date(2024, 1, 1), datetime.date(2024, 3, 21), "[{\"product\":\"Special Char\",\"amount\": 50}]", "Special character notes", "12345"),
    (9, "User with multi-byte chars: éàçüö", "multibyte@example.com",  "555-555-5555", "Multi-byte Corp",  "Multi-byte Expert",  "1 Multi-byte Way", "Multi-byte City", "MB",  "USA", "Multi-byte Industry", "Frank White", datetime.date(2024, 2, 15), datetime.date(2024, 3, 20), "[]", "Multi-byte notes", "54321"),
    # ...more special char records

]

# Combine all test data
data = data_happy_path + data_edge_cases + data_error_cases + data_null_handling + data_special_chars

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Display the DataFrame (for verification)
df.show()

# Further processing (e.g., writing to Delta table, generating SQL INSERT statements)
# ...

