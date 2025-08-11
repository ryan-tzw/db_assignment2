import sys
from pyspark.sql import SparkSession, functions as sf

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

# copy csv into /assignment2/part1/input/
df = spark.read.option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/")

reviews_zip = df.select(
    sf.col("ID_TA"),
    sf.explode(
        sf.arrays_zip(
            sf.col("Reviews").getItem(0).alias("rtext"),
            sf.col("Reviews").getItem(1).alias("rdate")
        ).alias("reviews")
    )
)

# reviews_exploded = reviews_zip.select(
#     sf.col("ID_TA"),
#     sf.explode("reviews")
# )

result = reviews_zip.select(
    sf.col("ID_TA"),
    sf.col("reviews.rtext").alias("review"),
    sf.col("reviews.rdate").alias("date")
)

result.show(10)

result.write.option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/output/question3/")