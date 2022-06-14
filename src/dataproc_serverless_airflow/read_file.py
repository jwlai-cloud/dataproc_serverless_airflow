from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def retrieve_files(known_args):
    return [
        f'gs://{known_args.data_bucket}/{file}'
        for file in known_args.file_names.split(',')
    ]

def read_file(spark: SparkSession,
              files: str) -> DataFrame:
    """
    :param spark: Spark Context
    :param file_uri: URI of the Input File
    :return: Spark DataFrame
    """
    return (spark
            .read
            .option("delimiter", ",")
            .option("header", "true")
            .csv(files))
