from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder
    .appName("Iceberg Parquet Writer")
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.my_catalog.type", "hadoop")
    .config("spark.sql.catalog.my_catalog.warehouse", "s3://emr-project-raw/serverless_example/iceberg/")
    .getOrCreate()
)

data_path = 'data/data.csv'
# data_path = "s3://emr-project-raw/serverless_example/data/data.csv"

df = spark.read.options(header = True).csv(data_path)
df = (
    df.withColumn('ingestion_timestamp', lit(current_timestamp()))
      .withColumn('balance_eurocents', regexp_replace((col('balance')/100).cast('string'), '.', ','))
)

df.writeTo("my_catalog.db.processed_data").using("iceberg").createOrReplace()

# df.write.format('iceberg').mode('overwrite').save('s3://emr-project-raw/serverless_example/iceberg/processed_data')
# df.write.option('header', True).csv('s3://emr-project-raw/serverless_example/iceberg/processed_data', mode='overwrite')