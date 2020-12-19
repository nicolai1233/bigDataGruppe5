import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import lit, max
from pyspark import SparkConf
import pandas as pd
from pyspark.sql.functions import col, asc,desc
from DataFormatter import DataFormatter
from Converter import CountryConverter
import pyspark
from pyspark.sql import SparkSession
import pydoop.hdfs as hdfs
import subprocess


java8_location='/usr/lib/jvm/java-1.8.0-openjdk-amd64'
os.environ['JAVA_HOME'] = java8_location

if __name__ == "__main__":
    print("--main started---")
    #vars
    rawTweetDataLocation = "data/*"
    rawCovidDataLocation = "covidCases/ting.csv"
    df = DataFormatter()
    cc = CountryConverter()
    
    #here the methods are used to create 2 spark jobs that makes a spark sql and export it to HDFS
    formattedTweets = df.getTweetMetaData(rawTweetDataLocation)
    test = df.getCovidMetaDate(rawCovidDataLocation)
    

    
    # Is used to get the name and path for the sql for tweets
    p = subprocess.Popen("hdfs dfs -ls covidCases/tweetResult.csv/ |  awk '{print $8}'",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)

    # get the paths as a ellipsis
    path = p.communicate()[0]

    #p.terminate()
    
    # turn the ellipsis that countains the path to string
    location = ""
    for i in range(37,120):
        location = location +path[i]

    #print(location)
    
   
    dataTweets = {'country_acronym':[],'count':[]}
   
    #get the sql csv and add it to array 
    with hdfs.open(location) as f:
        for line in f:
            #print(line)
            mid = line.split(',')
            dataTweets['country_acronym'].append(mid[0])
            dataTweets['count'].append(mid[1])
            continue

    pp = subprocess.Popen("hdfs dfs -ls covidCases/covidNumbersFromEuTest.csv/ |  awk '{print $8}'",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)

    # get the paths as a ellipsis
    newpath = pp.communicate()[0]
    
    # turn the ellipsis that countains the path to string
    newlocation = ""
    for i in range(48,142):
        newlocation = newlocation +newpath[i]

    
    print(newlocation)
   
    EuCovidData = {'country':[],'death':[],'cases':[]}
    
    with hdfs.open(newlocation) as f:
        print(newlocation)
        for line in f:
            #print(line)
            newmid = line.split(',')
            EuCovidData['country'].append(newmid[0])
            EuCovidData['death'].append(newmid[1])
            EuCovidData['cases'].append(newmid[2])
            continue
         

    # clean the data and add to hdfs
    cleanTweets = cc.countryCodeToAcronym(dataTweets,EuCovidData)
    