# url --> wiki list of largest companies in us by revernue
# https://en.wikipedia.org/wiki/List_of_largest_companies_in_the_United_States_by_revenue

from bs4 import BeautifulSoup
import requests

url = "https://en.wikipedia.org/wiki/List_of_largest_companies_in_the_United_States_by_revenue"

page = requests.get(url)

soup = BeautifulSoup(page.text, 'lxml')

table = soup.find_all('table')[1]
table_prettified = soup.find_all('table')[1].prettify()
# prettified soup

titles = table.find_all('th')
# list of table headers

titles_content = [title.text.strip() for title in titles]

# print(titles_content)

# now that we got a list of headers in the table, now push it into the .csv file

# pip install PyPI
import pandas as pd

df = pd.DataFrame(columns = titles_content)
# The pandas DataFrame is a structure that contains two-dimensional data and its corresponding labels. DataFrames are widely used in data science, machine learning, scientific computing, and many other data-intensive fields. DataFrames are similar to SQL tables or the spreadsheets that you work with in Excel or Calc.

# print(df)
# print columns

# lets get the rows
t_data = table.find_all('tr')
rows_content = []
for row in t_data:
    row_data = row.find_all('td')
    row_content = [row.text.strip() for row in row_data]
    rows_content.append(row_content)

for row in rows_content:
    if len(row) > 0:
        df = df._append(pd.Series(row, index=df.columns), ignore_index=True)

print(df)

# saving the df into csv
# Assuming you already have the DataFrame 'df'
df.to_csv('output_file.csv', mode='w', index=False)
