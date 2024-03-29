# -*- coding: utf-8 -*-
"""Practical03_24.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1mKCX_-SkZl-yf5PhHYMk-a0p4Qtpj6qv
"""

import pandas as pd

from google.colab import drive
drive.mount('/content/drive')

!ls "/content/drive/My Drive/colab"

!ls "/content/drive/MyDrive/colab"

dataset=pd.read_csv('/content/drive/MyDrive/colab/dirtydata.csv',sep=',',header='infer')

toyo = pd.read_csv('/content/drive/MyDrive/colab/Toyota.csv',sep=',',header='infer')

print(toyo)

toyo.replace({'a' : {'č': 'c', 'Í': 'I'}}, regex = True)

toyo.isnull()

print(toyo.isnull().sum())

med = toyo[{"Age"}].median()
med

toyo["Age"].fillna(60, inplace = True)

print(toyo.isnull().sum())

toyo.loc['', 'Doors']=45

print(toyo['Doors'])

a = len(toyo['FuelType'])
print(a)

b = toyo['FuelType'].duplicated().sum()
print(b)

print(a-b)

toyo["FuelType"].fillna(0,inplace = True)

print(toyo.isnull().sum())

toyo['MetColor']

toyo[{'MetColor'}].mode()

toyo["MetColor"].fillna(1,inplace =True)

print(toyo.isnull().sum())

toyo['Fueltype'] = toyo['FuelType'].replace(['Petrol','Diesel','CNG'],[0,1,2])

print(toyo)

