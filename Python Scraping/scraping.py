import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_tables(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract data from all tables on the page
    all_tables = soup.find_all('table')

    table_data = []
    for table in all_tables:
        table_rows = table.find_all('tr')
        for row in table_rows:
            columns = row.find_all(['td', 'th'])
            if columns:
                table_data.append([col.text.strip() for col in columns])

    return table_data

scraped_data = scrape_tables("https://economictimes.indiatimes.com/wealth/insure/life-insurance/latest-life-insurance-claim-settlement-ratio-of-insurance-companies-in-india/articleshow/97366610.cms")
# print("hi",scraped_data)


# creating pandas dataframe
df = pd.DataFrame(scraped_data)

# arranging the table accordingly
# Get the number of rows and columns
num_rows, num_columns = df.shape

# row one transformed
df.iloc[0, 0] = df.iloc[0].str.cat()
# Drop columns starting from the 2nd column for the 1st row
df.iloc[0, 2:] = None

# row two transformed
df.iloc[1, 2] = df.iloc[1, 2] + df.iloc[1, 3]
# Drop the 2nd column in the first row
df = df.drop(df.index[0], axis=3)
df.iloc[1, 3] = df.iloc[1, 3] + df.iloc[1, 4]
# Drop the 2nd column in the first row
df = df.drop(df.index[1], axis=4)

# Specify the CSV file path
csv_file_path = 'scraped_data.csv'

# Write the DataFrame to the CSV file, ensuring the file is created anew
df.to_csv(csv_file_path, mode='w', index=False)

