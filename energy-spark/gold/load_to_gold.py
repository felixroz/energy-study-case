# import libraries
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark import SparkConf

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
        .config("spark.memory.offHeap.size","10mb") \
        .getOrCreate()

    # show configured parameters
    print(SparkConf().getAll())

    # improving debugging with jupyter
    # spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

    # set log level
    spark.sparkContext.setLogLevel("INFO")

    # set location of files
    # minio data lake engine

    # [landing zone area]
    # device and subscription
    silver_faker_location = "s3a://lakehouse/silver/faker/"

    # read device data
    # json file from landing zone
    df_faker = spark.read \
        .format("delta") \
        .load(silver_faker_location)

    # [silver zone area]
    # data lakehouse paradigm
    # need to read the entire landing zone
    # usual scenario but not the best practice
    write_delta_mode = "overwrite"
    delta_gold_zone = "s3a://lakehouse/gold"
    df_faker.write.mode(write_delta_mode).format("delta").save(delta_gold_zone + "/faker/")

    # stop session
    spark.stop()

