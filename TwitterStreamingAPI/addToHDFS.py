
import os
from pyspark.sql.functions import lit, max
from pyspark import SparkConf
import pandas as pd
import pyspark
import pydoop.hdfs as hdfs
from datetime import date
import csv 
from csv import reader


java8_location='/usr/lib/jvm/java-1.8.0-openjdk-amd64'
os.environ['JAVA_HOME'] = java8_location



if __name__ == '__main__':
    print("--addTOHDFS--")
    
    today = date.today()

    j = {'country_code':[],'count':[]}
    mild = {'country_code':[],'count':[]}
    # load csv to array
    with open(str(today)+"ting.csv", 'r') as read_obj:
        csv_reader = reader(read_obj)
        for row in csv_reader:
            j['country_code'].append(row[0])
            j['count'].append(row[1])
          
    # combine data
    for i in range(len(j['country_code'])):
        if j['country_code'][i] not in mild['country_code']:
            mild['country_code'].append(j['country_code'][i])
            mild['count'].append(j['count'][i])
        elif j['country_code'][i] in mild['country_code']:
            for k in range(len(mild['country_code'])):
                if(j['country_code'][i] == mild['country_code'][k]):
                    mild['count'][k] = int(float(mild['count'][k]))+int(float(j['count'][i]))       

    #create fill to send to hdfs
    l = ['country_code','count']
    name = str(today)+".csv"
    with open(name, "w") as outfile:
        #if file is empty create value 
        if(os.stat(str(today)+".csv").st_size == 0):
            writer = csv.writer(outfile)
            writer.writerow(l)   
        # add data to the new fill 
        print(range(len(mild)))
        for i in range(len(mild['country_code'])):
            writer = csv.writer(outfile)
            writer.writerow([mild['country_code'][i],(mild['count'][i])])

    print(mild)
    location = 'streamingData/'+name
        
    if hdfs.path.exists(location):
        hdfs.rm('streamingData/'+name)
        hdfs.put(name,'streamingData')
    else:
        hdfs.put(name,'streamingData')

    
    
   