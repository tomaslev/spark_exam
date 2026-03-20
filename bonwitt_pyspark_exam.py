from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf
import urllib.request

sc = SparkContext.getOrCreate(conf=conf)

# Define a SparkSession
spark = SparkSession \
    .builder \
    .master("local") \
    .appName("pyspark_exam") \
    .getOrCreate()
spark