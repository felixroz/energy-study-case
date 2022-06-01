# import libraries
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import DateType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import current_timestamp, col, split

from transform.datamask import MaskColumnFrom
from transform.date import DateConvertion
from transform.rename import rename_columns

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
    bronze_faker_location = "s3a://lakehouse/bronze/faker/"

    # read device data
    # json file from landing zone
    df_faker = spark.read \
        .format("delta") \
        .load(bronze_faker_location)

    # get number of partitions
    print(df_faker.rdd.getNumPartitions())

    # count amount of rows ingested from lake
    df_faker.count()

    # [data enrichment]
    # drop unwanted columns
    df_faker_cleaned = df_faker.drop("address.id", "id", "image", "website")

    # Transform birthday column into 'age' column
    df_faker_transformed = DateConvertion(df_faker_cleaned,'birthday').convert_birthday_to_age()
    # Categorizes ages into predetermined range of values
    df_faker_transformed = DateConvertion(df_faker_transformed,'age').convert_age_to_age_groups(keep_age_column=True)


    # Convert the column 'email' into a 'domain' column by removing everything before '@'
    df_faker_transformed = df_faker_transformed.withColumn('domain', split('email','@').getItem(1) )

    columns_to_mask_by_len = ['firstname','lastname','address.buildingNumber']
    # Mask columns using the class MaskColumn (to see how this works please, see docs)
    masked_transformed_df = MaskColumnFrom(df_faker_transformed).by_len(list_of_columns = columns_to_mask_by_len)

    masked_transformed_df = MaskColumnFrom(masked_transformed_df).before_a_character(list_of_columns = \
                                ['email'], character = '@')

    masked_transformed_df = MaskColumnFrom(masked_transformed_df).before_a_character(list_of_columns = \
                                ['address.zipcode'], character = '-')

    masked_transformed_df = MaskColumnFrom(masked_transformed_df).after_a_character(list_of_columns =\
                                ['address.latitude', 'address.longitude'], character = '.')

    masked_transformed_df = MaskColumnFrom(masked_transformed_df).keep_first(list_of_columns =\
                                ['phone'], length=4)

    renamed_columns_df = rename_columns(masked_transformed_df)

    # [silver zone area]
    # data lakehouse paradigm
    # need to read the entire landing zone
    # usual scenario but not the best practice
    write_delta_mode = "overwrite"
    delta_silver_zone = "s3a://lakehouse/silver"
    renamed_columns_df.write.mode(write_delta_mode).format("delta").save(delta_silver_zone + "/faker/")

    # stop session
    spark.stop()

