from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder \
    .appName("ReadParquet") \
    .master("local") \
    .getOrCreate()

bad_records_path = "/home/gson/GIT/P-SPARK/4-readWriteParquet"

# Specify options for reading the CSV file
options = {
    "inferSchema": "true",  # Corrected option name
    "mode": "dropmalformed",
    "header": "true",
    "escape": "\"",  # Removed quotes around the escape character
    "badRecordsPath": bad_records_path
}

# Read the CSV file
df = spark.read.format("csv") \
    .options(**options) \
    .load("/home/gson/GIT/P-SPARK/4-readWriteParquet/currency.csv")

# Show the DataFrame
df.show()

# Load the corrupt records
corrupt_records = spark.read.text(bad_records_path)

# Show the corrupt records
corrupt_records.show()