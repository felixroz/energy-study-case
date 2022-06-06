# import libraries
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, date_format, current_timestamp,to_date
from pyspark import SparkConf
import databricks.koalas as ks
# main spark program
# init application
if __name__ == '__main__':

    # init session
    # set configs
    spark = SparkSession \
        .builder \
        .appName("load-to-bronze") \
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
        .config("spark.memory.offHeap.size","10mb") \
        .getOrCreate()

    # improving debugging with jupyter
    # spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

    # set log level
    # spark.sparkContext.setLogLevel("INFO")

    # [silver zone area]
    # device and subscription
    silver_location = "s3a://lakehouse/silver/"

    # read oil
    # delta files from silver zone
    df_fuel_sales_raw = spark.read \
        .format("delta") \
        .load(silver_location + '/fuel_sales/')

    # select only columns that we want to serve in gold
    df_fuel_sales_cleaned = df_fuel_sales_raw.select(
        date_format(
            to_date('year_month','yyyy-MM')
            , 'yyyy-MM').alias('year_month'),
        'uf',
        'product',
        'unit',
        col('volume').cast('double'),
        col('created_at').cast('timestamp'),
        'load_date'
    )

    # [gold zone area]
    # data lakehouse paradigm
    # need to read the entire landing zone
    # usual scenario but not the best practice
    write_delta_mode = "overwrite"
    delta_gold_zone = "s3a://lakehouse/gold"
    
    df_fuel_sales_cleaned.write.mode(write_delta_mode).format("delta")\
        .partitionBy("load_date")\
        .save(delta_gold_zone + "/fuel_sales_by_uf_type_and_year/")

    # stop session
    spark.stop()

