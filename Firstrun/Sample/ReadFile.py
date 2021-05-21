'''
Created on May 8, 2021

@author: SK050763
'''
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#from pyspark.ml.param._shared_params_code_gen import header

spark= SparkSession.builder.appName("Databricks").getOrCreate()
sc =spark.SparkContext()

coronaDF = sc.read\
           .option("header", True)\
           .option("inferSchema", True)\
           .csv("C:/Users/SK050763/Downloads/archive")
           
           
coronaDF.show(20)