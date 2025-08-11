import sys
from pyspark.sql import SparkSession, functions as sf
from pyspark.sql.types import ArrayType, StringType

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

# copy csv into /assignment2/part1/input/
df = spark.read.option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/")

array_schema = ArrayType(StringType())
df = df.withColumn("Cuisine Style", sf.from_json(sf.col("Cuisine Style"), array_schema))

df_city_cuisine = df.select(
    sf.col("City"),
    sf.explode("Cuisine Style").alias("Cuisine")
)

df_result = df_city_cuisine.groupBy("City", "Cuisine").count().orderBy("City", "Cuisine")

df_result.show(10)

df_result.write.option("header", True).csv(f"hdfs://{hdfs_nn}:9000/assignment2/output/question4/")