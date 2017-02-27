
# coding: utf-8

# In[10]:

#!/usr/bin/env python

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
            
def generate_url(year):
    url_list=list()
    #generate the url for fetching the log files for every month's first day
    number_of_months=1
    while number_of_months < 13:
        if(number_of_months <10):
            url="http://www.sec.gov/dera/data/PublicEDGAR-log-file-data/"+year+"/Qtr1/log"+year+'%02d' % number_of_months+"01.zip"
            
        else:
            url="http://www.sec.gov/dera/data/PublicEDGAR-log-file-data/"+year+"/Qtr1/log"+year+str(number_of_months)+"01.zip"

        url_list.append(url)
        number_of_months=number_of_months+1

    print(url_list)
    #maybe_download("http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2016/Qtr1/log20160101.zip")
    
def maybe_download(url, year):
    
    import urllib.request
    import requests
    import os.path
    import zipfile
    import pandas as pd
   
    #fetching the zip file name from the URL
    file_name=url.split("/")
    
    #Downloading data if not already present in the cache
    if(os.path.exists("Part_2_log_datasets/"+file_name[8])):
        print("Already present")
        
    else:
        urllib.request.urlretrieve(url, "Part_2_log_datasets_trial/"+year+"/"+file_name[8])
        print("Download complete")
        
    #unzip the file and fetch the csv file
    zf = zipfile.ZipFile("Part_2_log_datasets_trial/"+year+"/"+file_name[8]) 
    csv_file_name=file_name[8].replace("zip", "csv")
    zf_file=zf.open(csv_file_name)
    
    #create a dataframe from the csv
    df = pd.read_csv(zf_file)
  
if __name__ == '__main__':
    fetch_year()
    
   
    
    
   
    
