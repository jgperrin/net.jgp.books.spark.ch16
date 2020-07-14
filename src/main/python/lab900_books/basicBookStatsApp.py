"""
* CSV ingestion in a dataframe.
*
* @author rambabu.posa
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (split,col)
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/goodreads/books.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Basic book stats") \
    .master("local[*]").getOrCreate()

# Reads a CSV file with header, called books.csv, stores it in a
# dataframe
df = spark.read.csv(header=True, inferSchema=True, path=absolute_file_path)

df = df.withColumn("main_author", split(col("authors"), "-").getItem(0))

# Shows at most 5 rows from the dataframe
df.show(5)

# Good to stop SparkSession at the end of the application
spark.stop()