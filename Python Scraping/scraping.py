import requests
from bs4 import BeautifulSoup
import json
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
print("hi",scraped_data)

df = pd.DataFrame(scraped_data)

# Specify the CSV file path
csv_file_path = 'scraped_data.csv'

# Write the DataFrame to the CSV file, ensuring the file is created anew
df.to_csv(csv_file_path, index=False, mode='w')

