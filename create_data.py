import pandas as pd
from faker import Faker
import boto3

# Initialize Faker
fake = Faker()

# Generate fake data
num_records = 1000

user_data = []
transaction_data = []
payment_data = []

for _ in range(num_records):
    user_data.append({
        'user_id': fake.unique.random_number(digits=6),
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'birth_date': fake.date_of_birth(),
        'email': fake.email(),
        'salary': fake.random_int(min=30000, max=100000),
        'is_verified': fake.random_element([True, False])
    })

    transaction_data.append({
        'transaction_id': fake.unique.random_number(digits=6),
        'created_at': fake.date_this_decade(),
        'user_id': fake.random_int(min=1, max=num_records),
        'recipient_bank': fake.random_element(['Bank A', 'Bank B', 'Bank C']),
        'account_number': fake.random_int(min=100000, max=999999),
        'amount': fake.random_int(min=10, max=1000),
        'admin_fee': fake.random_int(min=1, max=50),
        'unique_code': fake.uuid4(),
        'transaction_status': fake.random_element(['Success', 'Pending', 'Failed'])
    })

    payment_data.append({
        'payment_id': fake.unique.random_number(digits=6),
        'send_at': fake.date_this_decade(),
        'transaction_id': fake.random_int(min=1, max=num_records),
        'payment_method': fake.random_element(['Credit Card', 'Bank Transfer']),
        'account_number': fake.random_int(min=100000, max=999999),
        'payment_status': fake.random_element(['Paid', 'Pending', 'Failed'])
    })

# Create DataFrames
user_df = pd.DataFrame(user_data)
transaction_df = pd.DataFrame(transaction_data)
payment_df = pd.DataFrame(payment_data)

# Save DataFrames to Parquet files
user_df.to_parquet('user_account.parquet', index=False)
transaction_df.to_parquet('transaction.parquet', index=False)
payment_df.to_parquet('payment.parquet', index=False)

# Initialize S3 client
s3 = boto3.client('s3', region_name='us-east-1')

# Specify your bucket name and folder paths
bucket_name = 'your-s3-bucket-name'
user_account_folder = 'user_account/'
transaction_folder = 'transaction/'
payment_folder = 'payment/'

# Upload DataFrames to S3
s3.upload_file('user_account.parquet', bucket_name, user_account_folder + 'user_account.parquet')
s3.upload_file('transaction.parquet', bucket_name, transaction_folder + 'transaction.parquet')
s3.upload_file('payment.parquet', bucket_name, payment_folder + 'payment.parquet')

print("Parquet files uploaded to S3 successfully!")
