from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
import urllib.request as ur

#Q.1. Using the urlretrieve function from the urllib.request module, write a download_file function to download a filename from the previously mentioned global address. Apply this function to the files we want to download.
def file_dl(filename):
    url = "https://assets-datascientest.s3.eu-west-1.amazonaws.com/" + filename
    ur.urlretrieve(url, filename)
    
file_dl("gps_app.csv")
file_dl("gps_user.csv")

conf = SparkConf()
sc = SparkContext.getOrCreate(conf=conf)

# Define a SparkSession
spark = SparkSession \
    .builder \
    .master("local") \
    .appName("pyspark_exam") \
    .getOrCreate()
spark

raw_app = spark.read.option("header", True)\
                    .option("inferSchema", True)\
                    .option("escape", "\"")\
                    .csv("gps_app.csv")

raw_user = spark.read.option("header", True)\
                     .option("inferSchema", True)\
                     .option("escape", "\"")\
                     .csv("gps_user.csv")
                     
#Q.2. In an initial preprocessing step, rename all columns by replacing spaces with underscores and converting uppercase letters to lowercase.
for col in raw_app.columns:
   raw_app = raw_app.withColumnRenamed(col, col.replace(" ", "_").lower())
   
for col in raw_user.columns:
   raw_user = raw_user.withColumnRenamed(col, col.replace(" ", "_").lower())
   
#Q.3.1 Replace missing values in the rating column with the mean or median. Justify your choice.
#checking the skewdness of rating to determine whenther to use median or mean
raw_app.filter(~isnan("rating")).select(skewness("rating")).show()

#since skewdness is approx. 0.59 and therefore between -1 and 1, the correct replacement value is the mean
rating_avg = raw_app.filter(~isnan("rating")).select(avg("rating")).head()["avg(rating)"]

#now we can replace the NaN values with the average value of rating