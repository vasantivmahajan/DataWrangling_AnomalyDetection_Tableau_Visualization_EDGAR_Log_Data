
# coding: utf-8

# In[4]:

#log20030401
import pandas as pd

combined_df = pd.read_csv("2003/2003.csv") #  pass your 12 month combined csv here
# group by cik and date and get count of ciks for a date   
temp_df=combined_df.groupby(['cik','date'])['cik'].count()
temp_df.head()



# In[5]:

# convert group by result into a frame

grouped_frame = pd.DataFrame(temp_df.reset_index(name = "hit_count"))

grouped_frame  


# In[6]:

## Monitor change in hit count

def get_percent_change(curr, prev):
        change_in_perc = ((curr - prev)/prev ) * 100
        return change_in_perc

count = 0
analysis_df = pd.DataFrame()
frame_count = 0
for row in grouped_frame['cik']:
    current_cik = grouped_frame['cik'][count]
    current_hit_count = grouped_frame['hit_count'][count]
    current_date = grouped_frame['date'][count]
    if(count >= 1):
        if(current_cik == grouped_frame['cik'][count-1]):
            change_in_count = current_hit_count - grouped_frame['hit_count'][count-1] 
            change_in_perc = get_percent_change(current_hit_count,grouped_frame['hit_count'][count-1])
            
            if(change_in_perc >= 1000 ): ## decide on threshold
                analysis_df.loc[frame_count, 'cik'] = current_cik
                analysis_df.loc[frame_count, 'date'] = current_date
                analysis_df.loc[frame_count, 'change in %'] = change_in_perc
                frame_count += 1
                #print(current_cik ," changed by",change_in_perc," % on ",current_date)
                
    count +=1
    
analysis_df


# In[ ]:


# Load the data into a DataFrame
data = pd.read_csv('2003/log20030401.csv') # pass your single month stuff here
#grouping by IP
byIp = data.groupby('ip')
byIp


# In[ ]:

#grouping by IP
byIp = data.groupby('ip')
byIp


# In[ ]:

byCIK = data.groupby('cik')
byCIK['size'].max()


# In[ ]:

#getting requests with  status code 404
byIp404=data[data['code']==404]
byIp404
#Anamoly-request with 404 has a download size associated.


# In[ ]:

byIp404=data[data['code']==404].groupby('ip')
byIp404['size'].mean()


# In[ ]:

#1. Simple describe function on data
summary = data.describe()
summary
#Analysis: total number of requests on this day-  261289
#Average request per day :215
#Max file download size:5.259544e+07
#Avergae File donwload size:	1.703470e+05


# In[ ]:

import matplotlib.pyplot as plt
import numpy as np
get_ipython().magic('matplotlib inline')

from numpy import genfromtxt
from scipy.stats import multivariate_normal
from sklearn.metrics import f1_score


# In[ ]:

def read_dataset(filePath,delimiter=','):
    return pandas.read_csv(filePath)

def feature_normalize(dataset):
    mu = np.mean(dataset,axis=0)
    sigma = np.std(dataset,axis=0)
    return (dataset - mu)/sigma

def estimateGaussian(dataset):
    mu = np.mean(dataset, axis=0)
    sigma = np.cov(dataset.T)
    return mu, sigma
    
def multivariateGaussian(dataset,mu,sigma):
    p = multivariate_normal(mean=mu, cov=sigma)
    return p.pdf(dataset)

def selectThresholdByCV(probs,gt):
    best_epsilon = 0
    best_f1 = 0
    f = 0
    stepsize = (max(probs) - min(probs)) / 1000;
    epsilons = np.arange(min(probs),max(probs),stepsize)
    for epsilon in np.nditer(epsilons):
        predictions = (probs < epsilon)
        f = f1_score(gt, predictions, average = "binary")
        if f > best_f1:
            best_f1 = f
            best_epsilon = epsilon
    return best_f1, best_epsilon


# In[ ]:

tr_data = read_dataset('log20030401.csv') 
tr_data.head()


# In[ ]:

n_training_samples = tr_data['size'].shape[0]
n_training_samples
n_dim = tr_data['code'].shape[1]
n_dim


# In[ ]:


plt.figure()
plt.xlabel("Latency (ms)")
plt.ylabel("Throughput (mb/s)")
plt.plot(tr_data[:,0],tr_data[:,1],"bx")
plt.show()


# In[ ]:

dataCIK = pandas.read_csv('CIK-mapping.csv')
 
dataCIK.head(n=10)
dataCIK.dtypes
dataCIK['CIK']
dataCIK['CIK'].astype(np.float64)


# In[ ]:

mergedDf=pd.merge(dataCIK, data, left_on='CIK', right_on='cik')


# In[ ]:

data.head()


# In[ ]:

data.dtypes


# In[ ]:

byIp=mergedDf.groupby('ip')
byIp['company'].describe()


# In[ ]:

mergedDf.groupby('company').describe()


# In[ ]:

rows = np.random.choice(mergedDf.index.values, 10)
sampled_df = mergedDf.ix[rows]
sampled_df


# In[50]:

sampled_df.groupby('company').describe()


# In[ ]:



