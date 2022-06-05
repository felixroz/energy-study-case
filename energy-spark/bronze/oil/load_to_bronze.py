# import libraries
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark import SparkConf
import databricks.koalas as ks

# main spark program
# init application
if __name__ == '__main__':

    # init session
    # set configs
    spark = SparkSession \
        .builder \
        .appName("oil-data-extraction") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://35.232.202.106") \
        .config("spark.hadoop.fs.s3a.access.key", "myaccesskey") \
        .config("spark.hadoop.fs.s3a.secret.key", "mysecretkey") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.fast.upload", True) \
        .config("spark.hadoop.fs.s3a.multipart.size", 104857600) \
        .config("fs.s3a.connection.maximum", 100) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.memory.offHeap.enabled","true")  \
        .config("spark.memory.offHeap.size","100mb") \
        .getOrCreate()

    # show configured parameters
    print(SparkConf().getAll())

    # set log level
    spark.sparkContext.setLogLevel("INFO")

    # set location of files
    # minio data lake engine

    # [staging area]
    # oil csv file

    sdf_oil_raw = spark.read.format("com.databricks.spark.csv").option("header","true").load("s3a://staging/oil")

    sdf_oil_raw = sdf_oil_raw.coalesce(1)

    # [write to lakehouse]
    # [bronze zone area]
    # data lakehouse paradigm
    # need to read the entire landing zone
    # usual scenario but not the best practice
    write_delta_mode = "overwrite"
    delta_bronze_zone = "s3a://lakehouse/bronze"

    if DeltaTable.isDeltaTable(spark, delta_bronze_zone + "/oil/"):
        dt_diesel = DeltaTable.forPath(spark, delta_bronze_zone + "/oil/")
        dt_diesel.alias("historical_data")\
            .merge(
                sdf_oil_raw.alias("new_data"),
                '''
                historical_data.PRODUTO = new_data.PRODUTO 
                AND historical_data.created_at = new_data.created_at
                AND historical_data.VOLUME = new_data.VOLUME''')\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()
    else:
        sdf_oil_raw.write.mode(write_delta_mode)\
            .format("delta")\
            .partitionBy("load_date")\
            .save(delta_bronze_zone + "/oil/")

    # stop session
    spark.stop()

