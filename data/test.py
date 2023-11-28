from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
                    .master("local[4]")\
                    .appName("trans")\
                    .getOrCreate()
# sc =spark.sparkContext                   
opt = {"header":True, "escape":"\"", "inferschema":True, "mode":"permissive"}
df = spark.read.format("csv")\
                .options(**opt)\
                .load("data/employee_data.csv")
# print(df.rdd.getNumPartitions())
df = df.repartition(2)
# spark.conf.set('spark.sql.shuffle.partitions', '2')
# print(df.rdd.getNumPartitions())
df = df.filter(df["Age_in_Yrs"] < 30)\
        .select("First Name", "Last Name", "Gender")\
        .groupBy(df['Gender'])\
        .count()
# print(df.rdd.getNumPartitions())
df.collect()
input("enter anything to stop the application")