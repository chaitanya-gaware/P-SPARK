from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder\
                    .appName('movie')\
                    .master('local')\
                    .getOrCreate()

define the schema for u.data
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True),
    StructField("_corrupt_record", StringType(), True)
])

opt = {'header':False, 'sep':'\t', 'mode':'permissive', 'inferschema':False}
data_df = spark.read.format('csv')\
                    .options(**opt)\
                    .schema(schema)\
                    .load("/home/chaitanya/GIT/P-SPARK/case_study1/u.data")
# data_df.show(5)
data_df.count()


#define the schema for u.user
user_schema_field = [StructField('UserId', IntegerType(), True),
                     StructField('Age', IntegerType(), True),
                     StructField('Gender',StringType(), True),
                     StructField('Occupation',StringType(),True),
                     StructField('ZipCode', IntegerType(), True),
                     StructField('_corrupt_record',StringType(), True)]

user_schema = StructType(user_schema_field)

u_opt= {'header':False, 'sep':'|', 'mode':'permissive'}
user_df = spark.read.format('csv')\
                    .options(**u_opt)\
                    .schema(user_schema)\
                    .load('/home/chaitanya/GIT/P-SPARK/case_study1/u.user')

# user_df.show()                 
# print(user_df.count())

