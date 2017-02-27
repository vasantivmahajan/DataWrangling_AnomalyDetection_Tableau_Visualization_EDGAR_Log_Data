
# coding: utf-8

# In[3]:

###from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
#import urllib3 as ur
import urllib as ur

import os.path
import zipfile


# In[ ]:

def fetch_company_name_cik_table():
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
    ###return df


# In[ ]:

#!/usr/bin/env python       

class GetData:
    
    def __init__(self):
        """
        Retrieves and stores the urllib.urlopen object for a given url
        """
        
    def generate_url(self,year):
        
        #generate the url for fetching the log files for every month's first day
        number_of_months=1
        while number_of_months < 13:
            if(number_of_months <10):
                url="http://www.sec.gov/dera/data/PublicEDGAR-log-file-data/"+year+"/Qtr1/log"+year+'%02d' % number_of_months+"01.zip"
            else:
                url="http://www.sec.gov/dera/data/PublicEDGAR-log-file-data/"+year+"/Qtr1/log"+year+str(number_of_months)+"01.zip"
            number_of_months=number_of_months+1
        #temp_url=download_data("http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2016/Qtr1/log20160101.zip")
        return self.download_data("http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2003/Qtr1/log20030301.zip")
        
    def download_data(self,url):

        #fetching the zip file name from the URL
        file_name=url.split("/")
        
        self.create_directory("Part_2_log_datasets")

        #Downloading data if not already present in the cache
        if(os.path.exists("Part_2_log_datasets/"+file_name[8])):
            print("Already present")

        else:
            ur.request.urlretrieve(url, "Part_2_log_datasets/"+file_name[8])
            print("Download complete")

        #unzip the file and fetch the csv file
        zf = zipfile.ZipFile("Part_2_log_datasets/"+file_name[8]) 
        csv_file_name=file_name[8].replace("zip", "csv")
        zf_file=zf.open(csv_file_name)

        #create a dataframe from the csv
        df = pd.read_csv(zf_file)
        return df
    
    def create_directory(self,path):
        try:
            if not os.path.exists(path):
                os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise    
        
#fetch the year for which the user wants logs
year = input('Enter the year for which you need to fetch the log files: ')
#calling the function to generate dynamic URL
get_data_obj=GetData()
df=get_data_obj.generate_url(year)
        


# In[ ]:

#convert all the integer column in int format

df['zone'] = df['zone'].astype('int')
df['cik'] = df['cik'].astype('int')
df['code'] = df['code'].astype('int')
df['idx']=df['idx'].astype('int')
df['norefer']=df['norefer'].astype('int')
df['noagent']=df['noagent'].astype('int')
df['find']=df['find'].astype('int')
df['crawler']=df['crawler'].astype('int')
print(df.head(25))


# In[ ]:


#replacing empty strings with NaN 
df.replace(r'\s+', np.nan, regex=True)


# In[ ]:


#replace all ip column NaN value by a default ip address 
df["ip"].fillna("255.255.255.255", inplace=True)

#perform forward fill to replace NaN values by fetching the next valid value
df["date"].fillna(method='ffill')

#perform backward fill to replace NaN values by backpropagating and fetching the previous valid value
df["time"].fillna(method='bfill')

#replace all zone column NaN values by 'Not Available' extension
df["zone"].fillna("Not Available", inplace=True)

#replace all extension column NaN values by default extension
df["extention"].fillna("-index.htm", inplace=True)

#replace all size column NaN values by 0 and convert the column into integer 
df["size"].fillna(0, inplace=True)
df['size'] = df['size'].astype('int')

#replace all user agent column NaN values by the default value 1 (no user agent)
df["noagent"].fillna("Not Applicable", inplace=True)

#replace all find column NaN values by the default value 0 (no character strings found)
df["find"].fillna(0, inplace=True)

#replace all broser column NaN values by a string
df["browser"].fillna("Not Available", inplace=True)
df


# In[ ]:

# if the value in idx column is missing, check the value of the extension column, if its "-index.html" set the column's value 1 else 0
count=0
for i in df['idx']:
    if(np.isnan(i)):
        if(df['extension'][count]=="-index.htm"):
            i=1
        else:
            i=0
    count=count+1

# if the value of norefer column is missing, check the value of the find column, if it is 0, set the value 1, else it set it 0
counter=0
for i in df['norefer']:
    if(np.isnan(i)):
        if(df["find"][counter]==0):
            i=1
        else:
            i=0
    counter=counter+1
    
# if the value of crawler is missing, check the value of the code, if it is 404 set it as 1 else 0
count_position=0
for i in df['crawler']:
    if(np.isnan(i)):
        if(df["code"][count_position]==404):
            i=1
        else:
            i=0
    count_position=count_position+1


# In[ ]:

#insert a column to check CIK, Accession number discripancy
df.insert(6, "CIK_Accession_Anamoly_Flag", "N")


# In[ ]:


#check if CIK and Accession number match. The Accession number is divided into three parts, CIK-Year-Number_of_filings_listed.
#the first part i.e the CIK must match with the CIK column. If not, there exists an anomaly

count=0;
print("I am working")
for i in df['accession']:
    #fetch the CIK number from the accession number and convert it into integer
    list_of_fetched_cik_from_accession=[(int(i.split("-")[0]))]
    
    #check if the CIK number from the column and CIK number fetched from the accession number are equal
    if(df['cik'][count]!=list_of_fetched_cik_from_accession):
        df['CIK_Accession_Anamoly_Flag'][count]="Y"
        
    count=count+1
print("Done")
print(df.head(10))


# In[ ]:

#merge both the dataframe using the CIK,cik as common column

# read csv from source
company_df = pd.read_csv('CIK-mapping.csv') 

merged_df=pd.merge(company_df, df, left_on='CIK',right_on='cik' )
merged_df.head()
#merged_df.loc[merged_df['cik']==1438823]


# In[ ]:

print(df.head(10))


# In[ ]:

df.insert(7, "filename", "")


# In[ ]:

#Extension rule: if the file name is missing and only the file extension is present, then the file name is document accession number
count=0
for i in df["extention"]:
    if(i==".txt"):
        # if the value in extension is only .txt, fetch the accession number and append accession number to .txt
        #list_of_fetched_cik_from_accession=int(((df2["accession"].str.split("-")[count])[0]))
        #print((df["accession"]).astype(str))
        #list_of_fetched_cik_from_accession=int(df["accession"])
        df["filename"][count]=(df["accession"][count]).astype(str)+".txt"
    else:
        df["filename"][count]=i
    count=count+1
print(df.head(10))

