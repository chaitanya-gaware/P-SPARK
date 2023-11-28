from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType
from pyspark.sql.functions import *

spark = SparkSession.builder \
                .master("local")\
                .appName("withColumn")\
                .getOrCreate()
schema_field = [StructField("code",StringType(),True),
                StructField("symbol", StringType(),True),
                StructField("Name", StringType(),True),
                StructField("value",IntegerType(), True),
                StructField("_corrupt_records", StringType(), True)] 
schema = StructType(schema_field)
# opt = {"header":True, "escape":"\""}
opt = {"escape": "\"", "mode":"permissive"}
df = spark.read.format("csv") \
    .option("header", "true") \
    .options(**opt) \
    .schema(schema) \
    .load("/home/gson/GIT/P-SPARK/5-withColumn/currency.csv")

df.show()
df.withColumn("symbol", lit(2)).show() #updating the existing column in new datframe
df.show()
input("enter anything to exist")
