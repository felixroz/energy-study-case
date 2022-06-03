# import libraries
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import DateType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import current_timestamp, col
import databricks.koalas as ks

# main spark program
# init application
if __name__ == '__main__':

    # init session
    # set configs
    spark = SparkSession \
        .builder \
        .appName("load-to-bronze") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://165.227.255.79:9000") \
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

    # [landing zone area]
    # device and subscription
    get_staging_file_path = "s3a://staging/*.csv"

    # read device data
    # excel file from landing zone
    df_raw_fueltype_by_uf_and_product = ks.read_excel(  get_staging_file_path
                                                        , sheet_name='DPCache_m3')

    df_raw_diesel_by_uf_and_type = ks.read_excel(  get_staging_file_path
                                                        , sheet_name='DPCache_m3_3')

    # transforming koalas dataframes into spark dataframes
    df_raw_fueltype_by_uf_and_product = df_raw_fueltype_by_uf_and_product.to_spark()
    df_raw_diesel_by_uf_and_type = df_raw_diesel_by_uf_and_type.to_spark()


    # [bronze zone area]
    # data lakehouse paradigm
    # need to read the entire landing zone
    # usual scenario but not the best practice
    write_delta_mode = "overwrite"
    delta_bronze_zone = "s3a://lakehouse/bronze"

    df_raw_fueltype_by_uf_and_product.write.mode(write_delta_mode).format("delta").save(delta_bronze_zone + "/sales_fueltype_by_uf_and_product/")
    df_raw_diesel_by_uf_and_type.write.mode(write_delta_mode).format("delta").save(delta_bronze_zone + "/sales_diesel_by_uf_and_type/")

    # stop session
    spark.stop()

