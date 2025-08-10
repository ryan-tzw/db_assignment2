import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# you may add more import if you need to

# shorthand for hdfs namenode
# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

# copy csv into /assignment2/part1/input/
df = spark.read.option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/")
df.printSchema()

# removes rows with NO REVIEWS or RATING < 1.0
# cast to double cuz the values are strings by default
clean_df = df.filter(
    (col("Rating").cast("double") >= 1.0) &
    (col("Number of Reviews").cast("double") > 0)
)

clean_df.show(10)

clean_df.write.option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/output/question1/")
