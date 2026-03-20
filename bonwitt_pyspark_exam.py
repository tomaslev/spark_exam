from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf
import urllib.request as ur

def file_dl(filename):
    url = "https://assets-datascientest.s3.eu-west-1.amazonaws.com/" + filename
    ur.urlretrieve(url, filename)
    
file_dl("gps_app.csv")
file_dl("gps_user.csv")

sc = SparkContext.getOrCreate(conf=conf)

# Define a SparkSession
spark = SparkSession \
    .builder \
    .master("local") \
    .appName("pyspark_exam") \
    .getOrCreate()
spark