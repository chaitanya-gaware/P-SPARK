from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder\
                    .appName("withColumnRenamed")\
                    .master("local")\
                    .config("spark.executor.instances", 4)\
                    .getOrCreate()
                    
opt = {"escape":"\"","header":True,"mode":"permissive","inferSchema":False} #,, "inferSchema":False,"header":True, ,,
df = spark.read.format("csv")\
                .options(**opt)\
                .load("/home/gson/GIT/P-SPARK/6-withColumnRenamed/currency.csv")
df = df.withColumnRenamed("Symbol", "Sym")
df.show()
input("enter anything to stop the sparkApplication")