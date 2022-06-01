import databricks.koalas as pd
import pyspark.sql

def rename_columns(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

    """
    Fix dataframe columns name so it can't cause a conflict into
    the database

    Args:
        df(pyspark.sql.DataFrame): DataFrame to be fixed with the function

    Returns:
        df(pyspark.sql.DataFrame): DataFrame with fixed columns name
    """

    # Transforming Spark Dataframe to Koalas Dataframe
    df = pd.DataFrame(df)

    columns_to_be_renamed = df.columns

    # removes undesired characters
    renamed_columns = [i.upper().replace(' ','_').replace('.','_')
                        for i in columns_to_be_renamed]

    # Creates a dict to be parsed into pd.Dataframe().rename()
    dict_sourcecol_targetcol = dict(zip(columns_to_be_renamed, renamed_columns))

    # Renaming columns
    transformed_df = df.rename(columns=dict_sourcecol_targetcol)

    return transformed_df.to_spark()

