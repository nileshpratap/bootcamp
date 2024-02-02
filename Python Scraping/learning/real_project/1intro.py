# html stands for hyper text markup language: it is used to describe all of the elements on the web page.

# site used https://www.scrapethissite.com/pages/forms/

import requests
from bs4 import BeautifulSoup
# its a web scraping library

url = 'https://www.scrapethissite.com/pages/forms/'

page = requests.get(url)

# this takes all the html, and make it a beautiful readable soup
soup = BeautifulSoup(page.text, 'html')

# print(soup)
# its still looking a bit messy, lets prettify

print(soup.prettify())
# gives hierarchy as like html