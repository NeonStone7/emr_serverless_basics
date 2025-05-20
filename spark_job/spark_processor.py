from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (SparkSession.builder 
    .appName("Data Processing") 
    .getOrCreate() ) 

# data_path = 'data/data.csv'
data_path = "s3://emr-project-raw/serverless_example/data/data.csv"

df = spark.read.options(header = True).csv(data_path)
df = (
    df.withColumn('ingestion_timestamp', lit(current_timestamp()))
      .withColumn('balance_eurocents', regexp_replace((col('balance')/100).cast('string')), '.', ',')
)

df.write.format('iceberg').mode('overwrite').save('s3://emr-project-raw/serverless_example/iceberg/processed_data')
