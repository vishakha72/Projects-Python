import requests
from bs4 import BeautifulSoup

city = str(input("Enter city name "))

url = "https://www.google.com/search?q="+ "weather" + city

html = requests.get(url).content

soup = BeautifulSoup(html, 'html.parser')

temp = soup.find('div',attrs = {'class':'BNeawe iBp4i AP7Wnd'}).text

time_skyDescription = soup.find('div',attrs = {'class':'BNeawe tAd8D AP7Wnd'}).text

data = time_skyDescription.split('\n')

time = data[0]
sky = data[1]


listdiv = soup.findAll('div',attrs = {'class':'BNeawe s3v9rd AP7Wnd'})

strd = listdiv[5].text

pos = strd.find('Wind')
otherData = strd[pos: ]
print("Temperature is",temp)
print("Time:",time)
print("Sky Description",sky)
print(otherData)