from datetime import datetime
import math
import numpy as np
import databricks.koalas as pd # importing koalas as pd to minimize verbosity
import pyspark.sql
from pyspark.sql.functions import to_date, date_format, current_date, datediff, ceil, max
from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

class DateConvertion:

    def __init__(   self
                    , sdf: pyspark.sql.DataFrame
                    , column: str):
        """
        Every method of this class is in charge of converting a date column
        into a  
        """

        self.df = sdf
        self.column = column

    def convert_birthday_to_age(   self
                                    ,keep_birthday_column: bool = False) -> pyspark.sql.DataFrame:
        """
        This function is in charge of converting a birthday column into a
        column with the age
        Args:
            sdf (pd.DataFrame): Spark dataframe that will be transformed
            column (str): name of the birthday column

        Returns:
            df(pd.DataFrame): Spark Dataframe with the "age" column
        """
        df = self.df
        column = self.column

        # Create a datetime birthday column
        df = df.withColumn(f"{column}_date", to_date(df[column])) 

        # Create a column with the current date
        df = df.withColumn('load_date', current_date())

        # Get the difference in days between current date and birthday_date
        df = df.withColumn(f'difference_in_days',  datediff(df['load_date'], to_date(df[f"{column}_date"])))
        # Convert days to years, resulting in the age
        df = df.withColumn(f'age',  ceil(df['difference_in_days']/365.25))
        # Removes auxiliar column 'difference_in_days'
        df = df.drop('difference_in_days')
        # Removes auxiliar column '{column}_date'
        df = df.drop(f'{column}_date')

        # Removing 'birthday' column accordingly to the user's choice
        if keep_birthday_column == False: transformed_df = df.drop(column)

        return df


    def convert_age_to_age_groups(  self
                                    , keep_age_column: bool = False) -> pyspark.sql.DataFrame:
        """
        This function is in charge of dividing an age column into categories
        of '10 years age group' (i.e someone with 31 years old must be categorized in [30-40] age group category).
        

        Args:
            df (pd.DataFrame): pandas dataframe that will be transformed
            column (str): name of the age column
            keep_age_column (bool): Set this parameter to True if you want to keep the column
                                        used in this transformation
                                        
        Returns:
            df(pd.DataFrame): Dataframe with the "age_group" column
        """
        df = self.df
        column = self.column

        # Gets the maximum value of the column
        max_age = df.select(max(column)).collect()[0].__getitem__(f'max({column})')

        # Here I had to add + 11, (1) to include the max number in the interval
        # (10) to create one more category from (max range) to (max range + 9)
        # If max_age = 60, for example, we will have a category 60-69
        bins = [i for i in range(0, max_age+11, 10)]
        
        # Dividing labels using bins
        labels = [f'{i} - {i+9}' for i in bins[:-1]]

        # [classifying age in age groups]
        # Bucketizering age groups
        bucketizer = Bucketizer(splits=bins,inputCol=column, outputCol="buckets")
        df_buck = bucketizer.setHandleInvalid("keep").transform(df)

        # Defines a list with the number of buckets
        buckets = [float(i) for i in range(len(bins))]
        # Defines a dict with the number of the bucket and the corresponding label
        bucket_names = dict(zip(buckets,labels))

        udf_foo = udf(lambda x: bucket_names[x], StringType())
        transformed_df = df_buck.withColumn("age_groups", udf_foo("buckets"))

        # Cleaning auxiliary columns
        transformed_df = transformed_df.drop('buckets')

        # Removing 'age' column accordingly to the user's choice
        if keep_age_column == False: df = transformed_df.drop(column)
        
        return(transformed_df)
