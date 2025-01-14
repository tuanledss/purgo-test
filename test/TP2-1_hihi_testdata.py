import pandas as pd
import numpy as np

# Load the dataset
csv_file_path = '/dbfs/FileStore/winequality-white.csv'
df = pd.read_csv(csv_file_path)

# Convert 'quality' to binary 'high_quality'
# Assuming quality >= 7 is high quality
df['high_quality'] = (df['quality'] >= 7).astype(int)

# Split the dataset into training, validation, and test sets
# Assuming 70% training, 15% validation, 15% test
train_df = df.sample(frac=0.7, random_state=42)
remaining_df = df.drop(train_df.index)
validation_df = remaining_df.sample(frac=0.5, random_state=42)
test_df = remaining_df.drop(validation_df.index)

# Generate test data
def generate_test_data(df, num_records):
    test_data = []
    for _ in range(num_records):
        record = {
            'fixed_acidity': np.random.uniform(df['fixed acidity'].min(), df['fixed acidity'].max()),
            'volatile_acidity': np.random.uniform(df['volatile acidity'].min(), df['volatile acidity'].max()),
            'citric_acid': np.random.uniform(df['citric acid'].min(), df['citric acid'].max()),
            'residual_sugar': np.random.uniform(df['residual sugar'].min(), df['residual sugar'].max()),
            'chlorides': np.random.uniform(df['chlorides'].min(), df['chlorides'].max()),
            'free_sulfur_dioxide': np.random.uniform(df['free sulfur dioxide'].min(), df['free sulfur dioxide'].max()),
            'total_sulfur_dioxide': np.random.uniform(df['total sulfur dioxide'].min(), df['total sulfur dioxide'].max()),
            'density': np.random.uniform(df['density'].min(), df['density'].max()),
            'pH': np.random.uniform(df['pH'].min(), df['pH'].max()),
            'sulphates': np.random.uniform(df['sulphates'].min(), df['sulphates'].max()),
            'alcohol': np.random.uniform(df['alcohol'].min(), df['alcohol'].max()),
            'high_quality': np.random.choice([0, 1])  # Randomly assign high_quality
        }
        test_data.append(record)
    return pd.DataFrame(test_data)

# Generate 20-30 records for each condition
# Validate data preprocessing: binary conversion of 'quality'
test_data_preprocessing = generate_test_data(df, 25)

# Validate training data split
test_data_training_split = generate_test_data(train_df, 25)

# Validate validation data split
test_data_validation_split = generate_test_data(validation_df, 25)

# Validate test data split
test_data_test_split = generate_test_data(test_df, 25)

# Print sample test data
print(test_data_preprocessing.head())
print(test_data_training_split.head())
print(test_data_validation_split.head())
print(test_data_test_split.head())
