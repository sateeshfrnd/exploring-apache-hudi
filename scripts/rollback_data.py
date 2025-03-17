'''
PySpark script to demonstrate how to add records, upsert records, and rollback to the latest commit in Apache Hudi.

'''
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark Session
spark = (
    SparkSession.builder 
    .appName("Hudi Rollback Example") 
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") 
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") 
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") 
    .getOrCreate()
)

# Sample data with 2 accounts
sample_data = [
    (101, "Satish Kumar", 500000.0),
    (102, "Ramya Sree", 100000.0)
]

# Schema for the data
schema = StructType([
    StructField("account_id", IntegerType(), False),  
    StructField("account_holder_name", StringType(), False),
    StructField("balance", DoubleType(), False)
])

# Create DataFrame
df = spark.createDataFrame(sample_data, schema)
# TODO : Debug logs (Remove)
df.printSchema()
df.show(truncate=False)


# Hudi Table Configuration
table_name = 'bank_accounts'
database_name = 'hudi_demo'
hudi_options = {
    "hoodie.table.name": table_name,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "account_id",
    "hoodie.datasource.write.precombine.field": "account_id",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.hive_sync.enable": "true",
    "hoodie.datasource.write.hive_sync.mode": "glue",
    # "hoodie.datasource.write.hive_sync.mode": "hive"
    "hoodie.datasource.write.table.name": table_name,
    "hoodie.datasource.write.hive_sync.table": table_name,
    "hoodie.datasource.write.hive_sync.database": database_name,
    "hoodie.datasource.write.hive_style_partitioning": "false",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.table": table_name,
    "hoodie.datasource.hive_sync.db": database_name,
    "hoodie.datasource.hive_sync.mode": 'hms'
}

# S3 Path for Hudi Table
s3_datapath = "s3://hudi-data-bucket"

print('Step 01 : Write initial data to Hudi table')
(
    df.write.format("hudi") 
    .options(**hudi_options)
    .mode("overwrite") 
    .save(s3_datapath)
)

print("Initial data added to Hudi table.")

print("Check the data is loaded")
read_df = spark.read.format('hudi').load('path')
read_df.printSchema()
read_df.show(truncate=False)

print('Step 2: Upsert existing accounts and add a new account')
updated_data = [
    (101, "Satish Kumar", 500000.0),
    (102, "Ramya Sree", 150000.0), #  Update balance 
    (103, "Ruchit", 150000.0) # new record
]

upsert_df = spark.createDataFrame(updated_data, schema)
(
    upsert_df.write.format("hudi") 
    .options(**hudi_options) 
    .mode("append") 
    .save(s3_datapath)
)
print("Updated existing accounts and added a new account.")

print('Step 3: Rollback to the latest commit')
commits_df = spark.sql(f"show commits from '{s3_datapath}'")
commits_df.show(truncate=False)

# get the latest commit 
latest_commit = commits_df.orderBy("commit_time", ascending=False).first()["commit_time"]
print(f'latest_commit = {latest_commit}')

(
    spark.read.format("hudi") 
    .option("hoodie.datasource.read.instant.time", latest_commit) 
    .load(s3_datapath) 
    .write.format("hudi") 
    .options(**hudi_options) 
    .mode("overwrite") 
    .save(s3_datapath)
)

print(f"Rolled back to the latest commit: {latest_commit}")

print('Step 4: Query the table')
hudi_df = spark.read.format("hudi").load(s3_datapath)
hudi_df.show(truncate=False)