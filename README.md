# DataWrangling_AnomalyDetection_Tableau_Visualization_EDGAR_Log_Data

Steps to run Part-2 
Approach one:- 
1. Download the docker file from the repository.

2. Start docker, build the docker file. 
docker build -t Team1-Part-2 .
DockerFile downloads python image and installs required packages. It then clones part-2 repo copies required files to container and sets command to run part-1 python script.

3. After building docker file. Run following command 
docker run -ti Team1-Part-2

Enter year 

The Access Code and Secret Key are part of config.ini file.You can edit it on github and build docker file and run container.

Approach 2:

Download docker image from docker hub- 
docker pull vasantivmahajan/DataWrangling_AnomalyDetection_Tableau_Visualization_EDGAR_Log_Data 
