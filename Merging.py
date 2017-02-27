
# coding: utf-8

# In[66]:

import urllib.request
import requests
import os.path
import zipfile
import pandas as pd
import sys

def maybe_download(url_list, year):
   
    list=['df1','df2','df3', 'df4','df5','df6','df7','df8','df9','df10','df11','df12']
    year=str(year)
    count=0
    for i in url_list:
        
        #fetching the zip file name from the URL
        file_name=i.split("/")
        create_directory("Part_2_log_datasets_trial/"+year+"/")
        
        #Downloading data if not already present in the cache
        if(os.path.exists("Part_2_log_datasets_trial/"+year+"/"+file_name[8])):
            print("Data for ",file_name[8]," is already present, pulling it from cache")

        else:
            #pbar = ProgressBar(widgets=[Percentage(), Bar()])
  

            urllib.request.urlretrieve(i, "Part_2_log_datasets_trial/"+year+"/"+file_name[8], reporthook)
            print("Data for ",file_name[8],"not present in cache. Downloading data")

        #unzip the file and fetch the csv file
        zf = zipfile.ZipFile("Part_2_log_datasets_trial/"+year+"/"+file_name[8]) 
        csv_file_name=file_name[8].replace("zip", "csv")
        zf_file=zf.open(csv_file_name)
        
        #create a dataframe from the csv and append it to the list of dataframe
        list[count]=pd.read_csv(zf_file)
        count=count+1  
    
def create_directory(path):
        try:
            if not os.path.exists(path):
                os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

def generate_url(year):
    url_list=list()
    #generate the url for fetching the log files for every month's first day
    number_of_months=1
    
    while number_of_months < 13:
        #find the quarter for the month
        if number_of_months >= 1 and number_of_months < 4:
            quarter="Qtr1"
        elif(number_of_months >= 4 and number_of_months < 7):
            quarter="Qtr2"
        elif(number_of_months >= 7 and number_of_months < 10):
            quarter="Qtr3"
        elif(number_of_months >= 10 and number_of_months < 13):
            quarter="Qtr4"

        if(number_of_months <10):
            url="http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/"+str(year)+"/"+quarter+"/log"+str(year)+'%02d' % number_of_months+"01.zip"
            
        else:
            url="http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/"+str(year)+"/"+quarter+"/log"+str(year)+str(number_of_months)+"01.zip"

        url_list.append(url)
        number_of_months=number_of_months+1
    
    maybe_download(url_list,year)
    
def fetch_year():
    
     #fetch the year for which the user wants logs
    year = input('Enter the year for which you need to fetch the log files: ')
    year=int(year)
    if(year >= 2003 and year < 2016):
        #calling the function to generate dynamic URL
        generate_url(year)
    else:
        print("EDGAR log files are available for years 2003-2016. Kindly enter a year within this range")
        fetch_year()

fetch_year()

