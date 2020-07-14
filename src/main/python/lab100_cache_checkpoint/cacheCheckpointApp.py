"""
* Measuring performance without cache, with cache, and with checkpoint.
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
spark = SparkSession.builder.appName("Example of cache and checkpoint") \
    .config("spark.executor.memory", "70g") \
    .config("spark.driver.memory", "50g") \
    .config("spark.memory.offHeap.enabled", True) \
    .config("spark.memory.offHeap.size", "16g") \
    .master("local[*]").getOrCreate()

spark.sparkContext.setCheckpointDir("/tmp")


# Specify the number of records to generate
recordCount = 500000





def processDataframe(recordCount, mode, spark):
    pass
