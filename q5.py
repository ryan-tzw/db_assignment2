import sys 
from pyspark.sql import SparkSession
# you may add more import if you need to
import json
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import ArrayType, StringType
from itertools import combinations

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

#read file
input_path = f"hdfs://{hdfs_nn}:9000/assignment2/part2/input/"
df = spark.read.parquet(input_path)

#extract data
def extract_names(cast_json):
    try:
        cast_list = json.loads(cast_json)
        return [member['name'] for member in cast_list if 'name' in member]
    except:
        return []
    
extract_names_udf = udf(extract_names, ArrayType(StringType()))
df_with_cast = df.withColumn("actor_names", extract_names_udf(col("cast")))

def pair_finder(names):
    return [sorted(pair) for pair in combinations(names, 2)]

pair_finder_udf = udf(pair_finder, ArrayType(ArrayType(StringType())))
df_pairs = df_with_cast.withColumn("actor_pairs", pair_finder_udf(col("actor_names")))

#explode time
df_exploded = df_pairs.select(col("movie_id"), col("title"), explode(col("actor_pairs")).alias("pair"))\
    .select(col("movie_id"), col("title"), col("pair")[0].alias("actor1"), col("pair")[1].alias("actor2"))

#count
pair_counts = df_exploded.groupBy("actor1", "actor2").count().filter(col("count") > 1)

#join
qualified_pairs = pair_counts.select("actor1", "actor2")  # drop 'count'
result = qualified_pairs.join(df_exploded, ["actor1", "actor2"], "inner") \
    .select("movie_id", "title", "actor1", "actor2").dropDuplicates()

#writing data
output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question5/"
result.write.parquet(output_path)