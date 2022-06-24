from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

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

#filter for genres column and use fillna to replace
replace = udf(lambda x: "['elevator music']" if x == "[]" else x)
spark_df.select(replace(spark_df.genres)).show(10)