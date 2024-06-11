import pandas as pd
from faker import Faker

# Initialize Faker
fake = Faker()

# Generate fake data
num_records = 500

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

print("Synthetic data saved as Parquet files: user_account.parquet, transaction.parquet, payment.parquet")
