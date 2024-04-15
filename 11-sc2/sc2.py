from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("scd2").getOrCreate()

df = spark.read.format("csv").option("mode","permissive").option("header",True).load("11-sc2/data.csv") 
df.show()