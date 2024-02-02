# pip install beautifulsoup4
# pip install requests

import requests
from bs4 import BeautifulSoup

req = requests.get("https://www.geeksforgeeks.org/")

soup = BeautifulSoup(req.content, "html.parser")

res = soup.title

print(res.get_text())

# print(soup.prettify())

# print(soup.get_text())





