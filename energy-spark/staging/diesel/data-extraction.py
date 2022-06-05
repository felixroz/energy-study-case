# import libraries
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, current_date
import databricks.koalas as ks
import pandas as pd
import unicodedata

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

    # [extraction]
    # diesel df from url

    df_diesel_raw = pd.read_csv("https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/vendas-oleo-diesel-tipo-m3-2013-2022.csv"
                            , delimiter=";")

    # [adjusting column names]
    new_column_names = [unicodedata.normalize('NFKD', i.replace(" ", "_")).encode('ascii', 'ignore').decode() for i in df_diesel_raw.columns]

    # [converting pandas to spark]
    df_diesel_raw.columns = new_column_names

    kdf_diesel_raw = ks.from_pandas(df_diesel_raw)

    sdf_diesel_raw = kdf_diesel_raw.to_spark()

    sdf_diesel_raw = sdf_diesel_raw.withColumn("created_at", current_timestamp())
    sdf_diesel_raw = sdf_diesel_raw.withColumn("load_date", current_date())


    # [write to lake]
    # [staging area]
    write_mode = "overwrite"
    staging_path = "s3a://staging"

    sdf_diesel_raw.coalesce(1).write.mode(write_mode)\
        .format('com.databricks.spark.csv')\
        .partitionBy("load_date")\
        .save(staging_path + "/diesel/", header = True)

    # stop session
    spark.stop()

