'''
Created on May 8, 2021

@author: SK050763
'''
# import findspark
# findspark.init('C:\Apps\spark-3.0.1-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark import  SparkContext, SparkConf
#import os

spark = SparkSession.builder.getOrCreate()

sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster("local"))

myRdd = sc.paralellize([1,2,3,5,25,32,678,23,])

print(myRdd.take(5))


