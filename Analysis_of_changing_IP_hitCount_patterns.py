
# coding: utf-8

# In[29]:

import pandas as pd

combined_df = pd.read_csv("2003/2003.csv") #  from manually created merge of CSVs

sliced_df=combined_df.head(25)

#sliced_df

#print("hi")


# In[27]:

# group by cik and date and get count of ciks for a date   
temp_df=combined_df.groupby(['cik','date'])['cik'].count()
temp_df.head()


# In[32]:

# convert group by result into a frame

grouped_frame = pd.DataFrame(temp_df.reset_index(name = "hit_count"))

grouped_frame  
     
 


# In[34]:

## Monitor change in hit count

def get_percent_change(curr, prev):
        change_in_perc = ((curr - prev)/prev ) * 100
        return change_in_perc

count = 0
#columns = ['cik','date','change in perc']
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
                analysis_df.loc[frame_count, 'change in perc'] = change_in_perc
                frame_count += 1
                #print(current_cik ," changed by",change_in_perc," % on ",current_date)
                
    count +=1
    
analysis_df


# In[ ]:



