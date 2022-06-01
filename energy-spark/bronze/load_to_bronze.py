# import libraries
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import DateType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import current_timestamp, col

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
    get_faker_file = "s3a://staging/*.csv"

    # read device data
    # json file from landing zone
    df_faker = spark.read \
        .format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load(get_faker_file)

    # get number of partitions
    print(df_faker.rdd.getNumPartitions())

    # count amount of rows ingested from lake
    df_faker.count()

    # [bronze zone area]
    # data lakehouse paradigm
    # need to read the entire landing zone
    # usual scenario but not the best practice
    write_delta_mode = "overwrite"
    delta_bronze_zone = "s3a://lakehouse/bronze"
    df_faker.write.mode(write_delta_mode).format("delta").save(delta_bronze_zone + "/faker/")

    # stop session
    spark.stop()

