import pandas as pd
import random

# Function to generate random claims data
def generate_claims_rows(num_rows):
    data = {'CID': [], 'Status': [], 'CAmt': [], 'Location': []}

    for cid in range(1, num_rows + 1):
        status = random.choice(['Declined', 'Approved', 'Under Review'])
        c_amt = random.randint(1000, 5000)
        location = random.choice(['Mumbai', 'Pune', 'Delhi', 'Ayodhya', 'NYC', 'Lucknow'])

        data['CID'].append(cid)
        data['Status'].append(status)
        data['CAmt'].append(c_amt)
        data['Location'].append(location)

    return pd.DataFrame(data)

# Generate 100 rows of claims data
num_rows = 10000
claims_data = generate_claims_rows(num_rows)

# Save to CSV file
claims_data.to_csv('claims_data.csv', index=False)

# Print a preview of the generated data
print(claims_data)