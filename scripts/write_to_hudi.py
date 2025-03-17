from pyspark.sql import SparkSession

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("Write to Hudi") 
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") 
    .getOrCreate()
)

# Sample data
sample_data = [
    (101, "Satish Kumar", 500000.0),
    (102, "Ramya Sree", 100000.0)
]

# Schema for the table
schema = StructType([
    StructField("account_id", IntegerType(), False),  
    StructField("account_holder_name", StringType(), False),
    StructField("balance", DoubleType(), False)
])

# Create DataFrame
df = spark.createDataFrame(sample_data, schema)

# Write as Hudi Table
(
    df.write.format("hudi")
    .option("hoodie.table.name", "bank_accounts")
    .option("hoodie.datasource.write.recordkey.field", "account_id")
    .option("hoodie.datasource.write.precombine.field", "account_id")
    .mode("overwrite")
    .save("s3://hudi-data-bucket/bank_accounts")
)


read_df = spark.read.format('hudi').load('path')
read.printSchema()
read.show(truncate=False)