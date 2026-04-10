from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import functions as f
import urllib.request as ur
import numpy as np
from pyspark.sql.types import *

#function to be able to quickly check nr of nan values (self-defined)
def count_nulls(df):
    for column in df.columns:
        print("columns", column, "has", df.where(f.isnull(column)).count(), "NULLs and", df.where(f.isnan(column)).count(), "NaNs")

#function to summarize null and nan values in table form (course-defined)
def getMissingValues(dataframe):
  count = dataframe.count()
  columns = dataframe.columns
  nan_count = []
  # we can't check for nan in a boolean type column
  for column in columns:
    if dataframe.schema[column].dataType == BooleanType():
      nan_count.append(0)
    else:
      nan_count.append(dataframe.where(isnan(f.col(column))).count())
  null_count = [dataframe.where(f.isnull(f.col(column))).count() for column in columns]
  return([count, columns, nan_count, null_count])

def missingTable(stats):
  count, columns, nan_count, null_count = stats
  count = str(count)
  nan_count = [str(element) for element in nan_count]
  null_count = [str(element) for element in null_count]
  max_init = np.max([len(str(count)), 10])
  line1 = "+" + max_init*"-" + "+"
  line2 = "|" + (max_init-len(count))*" " + count + "|"
  line3 = "|" + (max_init-9)*" " + "nan count|"
  line4 = "|" + (max_init-10)*" " + "null count|"
  for i in range(len(columns)):
    max_column = np.max([len(columns[i]),\
                        len(nan_count[i]),\
                        len(null_count[i])])
    line1 += max_column*"-" + "+"
    line2 += (max_column - len(columns[i]))*" " + columns[i] + "|"
    line3 += (max_column - len(nan_count[i]))*" " + nan_count[i] + "|"
    line4 += (max_column - len(null_count[i]))*" " + null_count[i] + "|"
  lines = f"{line1}\n{line2}\n{line1}\n{line3}\n{line4}\n{line1}"
  print(lines)

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
print(raw_app_clean.where(f.isnan(f.col("rating")) | f.isnull(f.col("rating"))).count())

#Q.3.2 Replace the missing value in the type column with the most logical value. Justify your choice.
#checking value counts in the type column
raw_app_clean.groupBy("type") \
  .count() \
  .orderBy("count", ascending=False) \
  .show()
  
#most common value by far is "Free". Will replace missing values with this value.
type_mode = raw_app_clean.select(f.mode("type")).head()["mode(type)"] #extracting most common value: Free

raw_app_clean = raw_app_clean.withColumn(
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
raw_app_clean = raw_app_clean.withColumn(
    "content_rating",
    f.when(f.col("content_rating").isNull() | f.isnan(f.col("content_rating")) | (f.col("content_rating") == "NULL"), "Unrated") #reused above code to cover all bases
     .otherwise(f.col("content_rating"))
)

raw_app_clean.groupBy("content_rating") \
  .count() \
  .orderBy("count", ascending=False) \
  .show()

#Q.3.4 Replace the remaining missing values in the current_ver and android_ver columns with their respective modalities.
current_ver_mode = raw_app_clean.select(f.mode("current_ver")).head()["mode(current_ver)"] #extracting mode: 'Varies with device'
android_ver_mode = raw_app_clean.select(f.mode("android_ver")).head()["mode(android_ver)"] #extracting mode: '4.1 and up'

raw_app_clean = raw_app_clean.withColumn(
    "current_ver",
    f.when(f.col("current_ver").isNull() | (f.col("current_ver") == "NULL")| (f.col("current_ver") == "NaN"), current_ver_mode) #nan not caught by fillna() because PySpark doesn't replace NaN in string columns. This line addresses this issue + replaces 0 with 'Free'
     .otherwise(f.col("current_ver"))
)

raw_app_clean = raw_app_clean.withColumn(
    "android_ver",
    f.when(f.col("android_ver").isNull() | f.isnan(f.col("android_ver")) | (f.col("android_ver") == "NULL"), android_ver_mode)
     .otherwise(f.col("android_ver"))
)

#Q.3.5 Verify that there are no more missing values
count_nulls(raw_app_clean)

#Q.4.1 Examine the missing values present in this dataset. Are the missing values (nan) in each column all on the same rows?
count_nulls(raw_user)

mismatches1 = raw_user.filter(
    f.isnan(f.col("translated_review")) != f.isnan(f.col("sentiment"))
).count()

mismatches2 = raw_user.filter(
    f.isnan(f.col("sentiment_polarity")) != f.isnan(f.col("sentiment_subjectivity"))
).count()

mismatches3 = raw_user.filter(
    f.isnan(f.col("sentiment")) != f.isnan(f.col("sentiment_polarity"))
).count()

print(mismatches1, mismatches2, mismatches3)

#Q.4.2 Clean the missing values
#since we know now that we have rows where all except 'app' are nans, we can delete all of these rows
raw_user_clean = raw_user.filter(~f.isnan(f.col("translated_review")))

#looking at the 5 rows with NULL in translated_review, we can assume they have no value and can be removed
print(raw_user_clean.filter(f.isnull(f.col("translated_review"))).show())
raw_user_clean = raw_user_clean.filter(~f.isnull(f.col("translated_review")))

#checking that all missing values are gone
print(count_nulls(raw_user_clean))

#Q.5.1 Check if there are any non-numeric values in the sentiment_polarity and sentiment_subjectivity columns. To do this, you can filter the rows for which converting the column to double returns a missing value.

#checking for both nans and NULLs when converting to double. No missing values in either column
print(raw_user_clean.withColumn("sentiment_polarity", f.col("sentiment_polarity").cast("double")).filter(f.isnan(f.col("sentiment_polarity")) | f.isnull(f.col("sentiment_polarity"))).show()
)
print(raw_user_clean.withColumn("sentiment_subjectivity", f.col("sentiment_subjectivity").cast("double")).filter(f.isnan(f.col("sentiment_subjectivity")) | f.isnull(f.col("sentiment_subjectivity"))).show()
)

#Q.5.2 Convert the numeric columns to float format.
num_cols = ["sentiment_subjectivity", "sentiment_polarity"]

for column in num_cols:
    raw_user_clean = raw_user_clean.withColumn(column, f.col(column).cast("float"))

#Q.5.3 Replace special characters in the translated_review column with spaces. Then, replace all spaces longer than 2 characters with a single space. You can use the regexp_replace function from the pyspark.sql.functions collection for this task.
raw_user_clean = raw_user_clean.withColumn("translated_review", f.regexp_replace("translated_review", "[^a-zA-Z0-9 ]", " "))
raw_user_clean = raw_user_clean.withColumn("translated_review", f.regexp_replace("translated_review", " {2,}", " "))

#Q.5.4 Convert all characters in the translated_review column to lowercase.
raw_user_clean = raw_user_clean.withColumn("translated_review", f.lower(f.col("translated_review")))

#Q.5.5 Display the number of comments for each group of sizes ranging from 1 character to 10 characters.
result = (
    raw_user_clean.withColumn("char_count", f.length("translated_review"))
      .filter(f.col("char_count").between(1, 10))
      .groupBy("char_count")
      .count()
      .orderBy("char_count")
)

print(result.show())

#Q.5.6 Keep only the rows where the comment length is greater than or equal to 3.
raw_user_clean = raw_user_clean.filter(f.length("translated_review") >= 3)

#Q.5.7 Calculate the 20 most frequent words in positive comments. To do this, you can use the rdd attribute of the DataFrame, then extract the translated_review column before applying the MapReduce operation discussed in Chapter 2.
#couldn't figure this one out

#Q.6.1 Change the type of the reviews column to integer, transforming problematic rows if necessary.
#figured out the one row with a non-numeric character (3.0M). Removing 'M' before casting as integer
raw_app_clean = raw_app_clean.withColumn(
    "reviews",
    f.regexp_replace(f.col("reviews"), "M", "").cast("integer")
)

#Q.6.2 We will now convert the installs column to integer as well. To do this, use a regex similar to the ones used previously to replace all non-digit characters with an empty string. Before replacing the column, ensure there are no null values.
#checking no null values
count_nulls(raw_app_clean)

#removing digits and casting as integer
raw_app_clean = raw_app_clean.withColumn(
    "installs",
    f.regexp_replace(f.col("installs"), r"\D", "").cast("integer")
)

#Q.6.3 Perform a similar operation to transform the price column to double. Be careful to handle decimal numbers properly.
# Found one value that would be silently converted to null: 'Everything'. Converting that to 0 before casting to double
raw_app_clean = raw_app_clean.withColumn(
    "price",
    f.when(f.col("price") == "Everything", "0")
     .otherwise(f.col("price"))
)

#removing non-digit characters before casting to double
raw_app_clean = raw_app_clean.withColumn(
    "price",
    f.regexp_replace(f.col("price"), r"[^\d.]", "").cast("double")
)

#Q.6.4 Assuming the date in the last_updated column is in the format MMMM d, yyyy, convert this column to date format using the to_date function.
#checking if there are any values that would be silently converted to NULL
non_conforming = raw_app_clean.filter(
    f.to_date(f.col("last_updated"), "MMMM d, yyyy").isNull() 
    & f.col("last_updated").isNotNull()
)

non_conforming.select("last_updated").show()

#found one unusual value: "1.0.19". Not sure what to make of it, so I'm converting it to NULL
raw_app_clean = raw_app_clean.withColumn(
    "last_updated",
    f.when(f.col("last_updated") == "1.0.19", None)
     .otherwise(f.col("last_updated"))
)

#converting to date
raw_app_clean = raw_app_clean.withColumn(
    "last_updated",
    f.to_date(f.col("last_updated"), "MMMM d, yyyy")
)

#checking that conversion worked
raw_app_clean.printSchema()

raw_app_clean.write \
       .mode('overwrite') \
       .format("jdbc") \
       .option("url", "jdbc:mysql://localhost:3306/database") \
       .option("dbtable", "gps_app") \
       .option("user", "user") \
       .option("password", "password") \
       .save()

raw_user_clean.write \
        .mode('overwrite') \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/database") \
        .option("dbtable", "gps_user") \
        .option("user", "user") \
        .option("password", "password") \
        .save()











