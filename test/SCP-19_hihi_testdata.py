import pandas as pd
import numpy as np

# Generate synthetic wine quality data
np.random.seed(42)

# Number of records
n_records = 30

# Generate features
fixed_acidity = np.random.uniform(5, 10, n_records)
volatile_acidity = np.random.uniform(0.1, 0.5, n_records)
citric_acid = np.random.uniform(0, 0.5, n_records)
residual_sugar = np.random.uniform(1, 10, n_records)
chlorides = np.random.uniform(0.01, 0.1, n_records)
free_sulfur_dioxide = np.random.uniform(5, 50, n_records)
total_sulfur_dioxide = np.random.uniform(20, 150, n_records)
density = np.random.uniform(0.990, 1.000, n_records)
pH = np.random.uniform(2.8, 3.8, n_records)
sulphates = np.random.uniform(0.4, 1.2, n_records)
alcohol = np.random.uniform(8, 14, n_records)

# Generate quality scores
quality = np.random.randint(3, 9, n_records)

# Convert quality to binary high_quality
# Assuming quality >= 6 is high quality
high_quality = (quality >= 6).astype(int)  # Validate binary conversion condition

# Create DataFrame
wine_data = pd.DataFrame({
    'fixed_acidity': fixed_acidity,
    'volatile_acidity': volatile_acidity,
    'citric_acid': citric_acid,
    'residual_sugar': residual_sugar,
    'chlorides': chlorides,
    'free_sulfur_dioxide': free_sulfur_dioxide,
    'total_sulfur_dioxide': total_sulfur_dioxide,
    'density': density,
    'pH': pH,
    'sulphates': sulphates,
    'alcohol': alcohol,
    'quality': quality,
    'high_quality': high_quality
})

# Split data into training, validation, and test sets
train_data = wine_data.sample(frac=0.7, random_state=42)  # Validate data splitting condition
remaining_data = wine_data.drop(train_data.index)
validation_data = remaining_data.sample(frac=0.5, random_state=42)
test_data = remaining_data.drop(validation_data.index)

# Output the generated data
print("Training Data:")
print(train_data)
print("\nValidation Data:")
print(validation_data)
print("\nTest Data:")
print(test_data)
