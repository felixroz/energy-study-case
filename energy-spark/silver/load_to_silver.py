# import libraries
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, to_date, current_timestamp, regexp_replace
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

    # [bronze zone area]
    # device and subscription
    bronze_location = "s3a://lakehouse/bronze"

    # read oil
    # delta files from bronze zone
    df_oil_raw = spark.read \
        .format("delta") \
        .load(bronze_location + '/oil/')

    # read diesel
    # delta files from bronze zone
    df_diesel_raw = spark.read \
        .format("delta") \
        .load(bronze_location + '/diesel/')

    df_merged = df_oil_raw.unionByName(df_diesel_raw)

    df_merged.createOrReplaceTempView("MERGED_TABLE")

    df_merged_flagged = \
    spark.sql("""
        SELECT
            *, 
            CASE
                WHEN MES == "JAN" THEN "01"
                WHEN MES == "FEV" THEN "02"
                WHEN MES == "MAR" THEN "03"
                WHEN MES == "ABR" THEN "04"
                WHEN MES == "MAI" THEN "05"
                WHEN MES == "JUN" THEN "06"
                WHEN MES == "JUL" THEN "07"
                WHEN MES == "AGO" THEN "08"
                WHEN MES == "SET" THEN "09"
                WHEN MES == "OUT" THEN "10"
                WHEN MES == "NOV" THEN "11"
                WHEN MES == "DEZ" THEN "12"
                ELSE
                "MONTH-NOT-RECOGNIZED"
            END AS FLAGGED_MONTH
        FROM MERGED_TABLE
        """)

    df_merged_flagged_transformed = df_merged_flagged\
        .withColumn("year_month"
                                , to_date(
                                    concat(col("ANO"),lit("-"),col("FLAGGED_MONTH"))
                                              , format= "yyyy-MM")
                    )\
        .withColumn("created_at", current_timestamp())\
        .withColumn("volume", regexp_replace(
                                            col("vendas")
                                            , "[^A-Z0-9_]", ""
                                            ).cast("double")
                    )\
        .withColumn("unit", lit("m3"))
    
    columns_to_be_renamed = {
        "unidade_da_federacao": "uf",
        "produto": "product",
        "ano": "year",
        "mes": "month_br_pattern"
    }

    kdf = ks.DataFrame(df_merged_flagged_transformed)
    
    kdf.columns = [i.lower() for i in kdf.columns]

    kdf_renamed = kdf.rename(columns=columns_to_be_renamed)

    sdf_renamed = kdf_renamed.to_spark()

    # [silver schema]
    """"
    SCHEMA
    [('year', 'string'),
    ('month_br_pattern', 'string'),
    ('grande_regiao', 'string'),
    ('uf', 'string'),
    ('product', 'string'),
    ('vendas', 'string'),
    ('created_at', 'timestamp'),
    ('load_date', 'date'),
    ('flagged_month', 'string'),
    ('year_month', 'date'),
    ('volume', 'double'),
    ('unit', 'string')]
    """

    # [silver zone area]
    # data lakehouse paradigm
    # need to read the entire landing zone
    # usual scenario but not the best practice
    write_delta_mode = "overwrite"
    delta_silver_zone = "s3a://lakehouse/silver"
    
    sdf_renamed.write.mode(write_delta_mode).format("delta")\
        .partitionBy("load_date")\
        .save(delta_silver_zone + "/fuel_sales/")
    

    # stop session
    spark.stop()

