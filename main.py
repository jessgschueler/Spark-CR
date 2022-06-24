from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

#create instance of SparkSession
spark = SparkSession.builder.getOrCreate()

#create dataframe from spotify_artists.csv
spark_df = (spark.read.format("csv").options(header="true").load("data/spotify_artists.csv"))

#profile data

#description, shown vertically for better readability in the terminal
spark_df.describe().show(vertical=True)
#show schema
spark_df.printSchema()
#show top ten enttries in name and genres
spark_df.select(spark_df.name, spark_df.genres).show(10)

#clean data

#create udf to replace empty brackets in genres col
replace = udf(lambda x: "['elevator music']" if x == "[]" else x)
spark_df.select(replace(spark_df.genres)).show(10)
#cast artist_popularity and followers as int
spark_df = spark_df.withColumn('artist_popularity', spark_df['artist_popularity'].cast(IntegerType()))
spark_df = spark_df.withColumn('followers', spark_df['followers'].cast(IntegerType()))
#sort the data in descending order by number of followers
spark_df.sort('followers', ascending=False).show(20)
#create udf to divide artist_poularitry by 100
pop_percent = udf(lambda x: x/100)
#create 'popularity_percent' column
spark_df.withColumn('popularity_percent', pop_percent(spark_df['artist_popularity'])).show(10)

#extract information

#filter values of 'name' with 'Queen', limit 20
spark_df.filter("lower(name) LIKE '%queen%'").show(20)