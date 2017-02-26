
# coding: utf-8

# In[25]:

from bs4 import BeautifulSoup
import requests
import pandas as pd
import urllib.request as ur
CIKs = []
companyNames = []
path = '.'
files = ['cik-list.txt']
for f in files:
    
      with open (f, "r") as myfile:
        for line in myfile:
            #print(line)
            values=line.split(':')
            companyNames.append(values[len(values)-3])
            CIKs.append(values[(len(values)-2)].strip('0'))
    
        


df = pd.DataFrame({'CIK': CIKs, 'company': companyNames})
df.to_csv('CIK-mapping.csv')
df.head()


# In[15]:

df


# In[ ]:



