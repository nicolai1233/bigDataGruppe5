import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import lit, max
from pyspark import SparkConf
import pandas as pd
from pyspark.sql.functions import col, asc,desc

java8_location='/usr/lib/jvm/java-1.8.0-openjdk-amd64'
os.environ['JAVA_HOME'] = java8_location

class DataFormatter:
    
  def getCovidMetaDate(self,rawDataLocation):
    #set up spark
    spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
    sc = spark.sparkContext
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    sc.setLogLevel("ERROR")

    #load data
    covidNumbersFromEu = spark.read.option("inferSchema", "true").option("header", "true").csv(rawDataLocation)
    #sort
    countSorted = covidNumbersFromEu.sort("deaths")

    #search in data set
    covidNumbersFromEu.createOrReplaceTempView("covidNumbersFromEu")
    res= spark.sql("SELECT countriesAndTerritories, sum(deaths), sum(cases) FROM covidNumbersFromEu WHERE month = 4 GROUP BY countriesAndTerritories ORDER BY sum(deaths) DESC")


    #Show
    res.show(truncate=False)
    
    #export
    res.coalesce(1).write.option("header","true").option("sep",",").mode("overwrite").csv("covidCases/covidNumbersFromEuTest.csv")
    
    return res;

  def getTweetMetaData(self,rawDataLocation):
      
    #setup
    spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
    sc = spark.sparkContext
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    sc.setLogLevel("ERROR")

    #load data
    df1 = spark.read.option("inferSchema", "true").option("header", "true").csv(rawDataLocation)

    #get count 
    res = df1.select("country_code").groupBy("country_code").count()

    #sort
    res = res.orderBy(col("count").desc())

    #where
    res = res.filter("count != 1")
    res = res.filter("count != 2")

    res.show(truncate=False)
    
    #export
    res.coalesce(1).write.option("header","true").option("sep",",").mode("overwrite").csv("covidCases/tweetResult.csv")

    return res;