# import libraries
from delta.tables import DeltaTable
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
        .appName("diesel-data-extraction") \
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

    # set location of files
    # minio data lake engine

    # [staging area]
    # reading diesel csv file from staging

    sdf_diesel_raw = spark.read.format("com.databricks.spark.csv").option("header","true").load("s3a://staging/diesel")

    sdf_diesel_raw = sdf_diesel_raw.coalesce(1)

    # [write to lakehouse]
    # [bronze zone area]
    # data lakehouse paradigm
    # need to read the entire landing zone
    # usual scenario but not the best practice
    write_delta_mode = "overwrite"
    delta_bronze_zone = "s3a://lakehouse/bronze"

    if DeltaTable.isDeltaTable(spark, delta_bronze_zone + "/diesel/"):
        dt_diesel = DeltaTable.forPath(spark, delta_bronze_zone + "/diesel/")
        dt_diesel.alias("historical_data")\
            .merge(
                sdf_diesel_raw.alias("new_data"),
                '''
                historical_data.PRODUTO = new_data.PRODUTO 
                AND historical_data.created_at = new_data.created_at
                AND historical_data.VENDAS = new_data.VENDAS''')\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()
    else:
        sdf_diesel_raw.write.mode(write_delta_mode)\
            .format("delta")\
            .partitionBy("load_date")\
            .save(delta_bronze_zone + "/diesel/")

    # stop session
    spark.stop()

