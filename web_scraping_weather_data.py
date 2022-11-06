# -*- coding: utf-8 -*-
"""Web_scraping_weather_data.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1eBkurR8gVVWLmM5kvv7NoYUzwAT7pnmR
"""

import requests
from bs4 import BeautifulSoup

city = str(input("Enter city: "))

url = "https://www.google.com/search?q=" + "weather" + city

html = requests.get(url).content
soup = BeautifulSoup(html, 'html.parser')
temp = soup.find('div',attrs={'class':'BNeawe iBp4i AP7Wnd'}).text

time_skyDescription = soup.find('div',attrs={'class':'BNeawe tAd8D AP7Wnd'}).text

data = time_skyDescription.split('\n')

time = data[0]
sky = data[1]

listdiv = soup.find('div',attrs={'class':'BNeawe s3v9rd AP7Wnd'}).text
strd = listdiv[5]

pos = strd.find('Wind')
otherData = strd[pos: ]
print("Temperature is ", temp)
print("Time: ",time)
print("Sky Description ",sky)
print(otherData)

import calendar

yy = 2022
mm = 12
print(calendar.month(yy,mm))

