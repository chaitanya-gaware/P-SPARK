# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *

# spark = SparkSession.builder\
#                     .master("local[4]")\
#                     .appName("trans")\
#                     .getOrCreate()
# # sc =spark.sparkContext                   
# opt = {"header":True, "escape":"\"", "inferschema":True, "mode":"permissive"}
# df = spark.read.format("csv")\
#                 .options(**opt)\
#                 .load("data/employee_data.csv")
# # print(df.rdd.getNumPartitions())
# df = df.repartition(2)
# # spark.conf.set('spark.sql.shuffle.partitions', '2')
# # print(df.rdd.getNumPartitions())
# df = df.filter(df["Age_in_Yrs"] < 30)\
#         .select("First Name", "Last Name", "Gender")\
#         .groupBy(df['Gender'])\
#         .count()
# # print(df.rdd.getNumPartitions())
# df.collect()
# input("enter anything to stop the application")

from pyspark.sql import SparkSession
from 8-join import test  

test.function()

spark = SparkSession.builder \
    .appName('accumulator') \
    .master('local') \
    .getOrCreate()

sc = spark.sparkContext

# Read text file using RDD
file = sc.textFile("data/war_and_peace.txt")

# Create an accumulator for counting blank lines
blanklines = sc.accumulator(0)

# Function to extract blank lines and count them
def extract_blank_lines(line):
    global blanklines
    if line == "":
        blanklines += 1
    return line.split(" ")

# Apply the transformation using flatMap
final = file.flatMap(extract_blank_lines)

# Collect the results and show the non-blank lines split into words
result = final.collect()
print(f'Blank lines: {blanklines.value}')
# print(result[:2])  # Displaying the first two elements for illustration
