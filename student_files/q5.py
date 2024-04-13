import sys 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, countDistinct
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

conf = SparkConf().setAppName("Wordcount Application")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

df = spark.read.option("header",True).parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))

allCast = df.withColumn(
    "cast",
    F.from_json("cast", "array<struct<name:string>>")
).selectExpr(
    "movie_id", "title", "inline(cast)"
)

cast_pairs = allCast.alias("a") \
        .join(allCast.alias("b"), (col("a.movie_id") == col("b.movie_id")) & (col("a.name") < col("b.name"))) \
        .select( col("a.movie_id"), col("a.title"),col("a.name").alias("actor1"), col("b.name").alias("actor2"))

cast_pairs_count = cast_pairs.groupBy("actor1", "actor2") \
        .agg(countDistinct("movie_id").alias("co_cast_count")) \
        .filter(col("co_cast_count") >= 2)

result = cast_pairs.alias("a")\
          .join(cast_pairs_count.alias("b"), (col("a.actor1") == col("b.actor1")) & (col("a.actor2") == col("b.actor2"))) \
          .select( col("a.movie_id"), col("a.title"),col("a.actor1"), col("b.actor2"))


result.write.parquet("./assignment2/output/question5")