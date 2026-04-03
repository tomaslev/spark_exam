from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import functions as f
import urllib.request as ur
import numpy as np
from pyspark.sql.types import *

#function to be able to quickly check nr of nan values
def count_nulls(df):
    for col in df.columns:
        print("column:", col, "has", df.where(f.isnan(col) | f.isnull(col)).count(), "NaN")

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
raw_app.filter(~f.isnan("rating")).select(f.skewness("rating")).show()

#since skewdness is approx. 0.59 and therefore between -1 and 1, the correct replacement value is the mean
rating_avg = raw_app.filter(~f.isnan("rating")).select(f.avg("rating")).head()["avg(rating)"]
print(raw_app.where(f.isnull(raw_app["rating"])).count())

#now we can replace the NaN values with the average value of rating
raw_app_clean = raw_app.fillna(value=rating_avg, subset=["rating"])
print(raw_app_clean.where(f.isnan(col("rating")) | f.isnull(col("rating"))).count())

#Q.3.2 Replace the missing value in the type column with the most logical value. Justify your choice.
#checking value counts in the type column
raw_app.groupBy("type") \
  .count() \
  .orderBy("count", ascending=False) \
  .show()
  
#most common value by far is "Free". Will replace missing values with this value.
type_mode = raw_app.select(f.mode("type")).head()["mode(type)"] #extracting most common value: Free

raw_app_clean = raw_app.withColumn(
    "type",
    f.when(f.col("type").isNull() | f.isnan(f.col("type")) | (f.col("type") == "0"), type_mode) #nan not caught by fillna() because PySpark doesn't replace NaN in string columns. This line addresses this issue + replaces 0 with 'Free'
     .otherwise(f.col("type"))
)

raw_app_clean.groupBy("type") \
  .count() \
  .orderBy("count", ascending=False) \
  .show()

#Q.3.3 Display the unique values taken by the type column. What do you notice? Fix the issue. This will also resolve the missing values in the content_rating column.
#noticed and fixed type column issues in previous questions. Noticed that in content_rating we have a NULL value as opposed to NaN. Code below addresses this.
#decided to replace NULL with "Unrated" because it's closest semantically plus it does not make sense to give a spcific rating to an app without knowing what it really is
raw_app_clean = raw_app.withColumn(
    "content_rating",
    f.when(f.col("content_rating").isNull() | f.isnan(f.col("content_rating")) | (f.col("content_rating") == "NULL"), "Unrated") #reused above code to cover all bases
     .otherwise(f.col("content_rating"))
)

raw_app_clean.groupBy("content_rating") \
  .count() \
  .orderBy("count", ascending=False) \
  .show()

#Q.3.4 Replace the remaining missing values in the current_ver and android_ver columns with their respective modalities.