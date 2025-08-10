import sys
from pyspark.sql import SparkSession, functions as sf

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

# copy csv into /assignment2/part1/input/
df = spark.read.option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/")

df = df.filter(sf.col("Price Range").isNotNull())
df = df.withColumn("Rating", sf.col("Rating").cast("double"))

grouped = df.groupBy("City", "Price Range")

agg = grouped.agg(
    sf.max("Rating").alias("Max Rating"),
    sf.min("Rating").alias("Min Rating"),
).sort("City", "Price Range")

agg.show(10)

best = df.join(agg, on=["City", "Price Range"]).where(sf.col("Rating") == sf.col("Max Rating")).drop("Max Rating", "Min Rating")
worst = df.join(agg, on=["City", "Price Range"]).where(sf.col("Rating") == sf.col("Min Rating")).drop("Max Rating", "Min Rating")

# sometimes we get more than 1 best or worst, im just leaving it all in
# the qn doesnt specify to only take 1 or not so idk

# best.show(10)
# worst.show(10)

out = best.unionByName(worst).dropDuplicates().sort("City", "Price Range")

out.printSchema()
out.show(10)

out.write.option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/output/question2/")
