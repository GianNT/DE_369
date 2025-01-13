""" Module to convert GZ files to Parquet files using PySpark"""
import os
from datetime import datetime, timedelta
from typing import Optional
from log_files import LogFile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, lit, sum as sum_agg
from pyspark.sql.types import IntegerType, StringType


class GzToParquet:
    """
    A class:
      - Read gz file in gzfolder and convert to Parquet file with explicit schema DataFrame.
    Methods:
        convert_gzfile_parquet :
            - Read gz file in gzfolder: gz_raw folder, transform it,
              and then saves it as Parquet file to gz_to_parquet_day folder

        convert_gzfiles_day:
            - Checks if there are 24 files (representing 24 hours in a day).
              If enough files are present, processes them using convert_gzfile_parquet.
    """

    def __init__(self, log_file: LogFile, gzfolder: str, gz_to_parquet_folder: str):
        self.log_file = log_file
        self.gzfolder = gzfolder
        self.gz_to_parquet_folder = gz_to_parquet_folder
        #new add
        self.gzfile: Optional[str] = None
        self.spark: Optional[SparkSession] = None
        self.pagenames: Optional[list[str]] = None


    def convert_gzfile_parquet(self, gzfile: str, spark: SparkSession, pagenames: list[str]) -> None:
        """ Converts a .gz file to a Parquet file using Apache Spark.
        Parameter:
            gzfile    : the path to the gzipped file to process : gz_raw
            spark     : SparkSession, The SparkSession instance used to read and write data.
            pagenames :(list[str]), A list of valid page names used for filtering the data.
        Return:
            None: The method writes the result to a Parquet file but does not return any value.

            The result written to Parquet file is a dataframe: df_final
            df_final (DataFrame): The transformed and aggregated DataFrame with the following columns:
            - domain (str): The domain extracted from the data.
            - pagename (str): The page name extracted from the data.
            - sumpageviewcount (int): The aggregated sum of page views for each domain and page name.
            - date_collection (str): The date when the data was collected.
            - time_collection (str): The time when the data was collected.
        """
        self.gzfile = gzfile
        self.spark = spark
        self.pagenames = pagenames
        if gzfile:
            try:
                if len(gzfile.strip().replace(".gz", "").split('-')) != 3:
                    raise ValueError(f"Unexpected gzfile_name format: {gzfile}")
                _, date_str, time_str = gzfile.strip().replace(".gz", "").split('-')

                date_collection =(
                            (datetime.strptime(date_str, "%Y%m%d") - timedelta(days=1)).strftime("%Y-%m-%d")\
                            if time_str == "000000"\
                            else datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
                )
                time_collection = f'{time_str[:2]}:{time_str[2:4]}:{time_str[4:]}'

                output_parquetfile_path = os.path.join(
                    self.gz_to_parquet_folder, os.path.basename(gzfile).replace(".gz", "")
                    )

                # Skip processing if the Parquet file already exists
                if os.path.exists(output_parquetfile_path):
                    inform_message = f"Skipping {gzfile}: Parquet file already exists."
                    self.log_file.log_progress(inform_message)
                    print(inform_message)
                    return
                print(f"Processing {gzfile}")

            # Read the .gz file into a DataFrame
                df = spark.read.text(gzfile)

                # Transform the DataFrame
                df_split = df.withColumn("split_value", split(df["value"], " "))
                df_transformed = df_split.select(
                    df_split["split_value"].getItem(0).cast(StringType()).alias("domain"),
                    df_split["split_value"].getItem(1).cast(StringType()).alias("pagename"),
                    df_split["split_value"].getItem(2).cast(IntegerType()).alias("pageview"),
                    df_split["split_value"].getItem(3).cast(StringType()).alias("other")
                ).drop("other").filter(col("pagename").isin(pagenames))

                df_final = df_transformed.groupBy("domain", "pagename") \
                    .agg(sum_agg(col("pageview")).cast(IntegerType()).alias("sumpageviewcount")) \
                    .withColumn("date_collection", lit(date_collection).cast(StringType()))\
                    .withColumn("time_collection", lit(time_collection).cast(StringType())) \
                    .orderBy("domain", "pagename")

                # Ensure the output directory exists
                os.makedirs(self.gz_to_parquet_folder, exist_ok=True)
                # Write to Parquet on the driver node
                df_final.write.parquet(output_parquetfile_path)
            except ValueError as ve:
                error_message = f"ValueError processing {gzfile}: {ve}"
                self.log_file.log_progress(error_message)
                print(error_message)
            except IOError as ioe:
                error_message = f"IOError processing {gzfile}: {ioe}"
                self.log_file.log_progress(error_message)
                print(error_message)



    def convert_gzfiles_day(self, spark: SparkSession, pagenames: list[str]):
        """
        Converts `.gz` files in the specified folder to Parquet format for a given set of page names.

        This method looks for `.gz` files in the `self.gzfolder` directory, and if there are exactly 24 files,
        it processes each file by calling the `convert_gzfile_parquet` method. Any errors encountered during the
        conversion are logged and printed.

        Parameter:
            spark     : SparkSession, The SparkSession instance used to read and write data.
            pagenames : (list[str]), A list of valid page names used for filtering the data.

        Raises:
            Exception: If an error occurs while converting a `.gz` file to Parquet format, it logs and prints the error.
        """
        gzfile_list= [os.path.join(self.gzfolder, gzfile_basename)\
                                   for gzfile_basename in os.listdir(self.gzfolder)\
                                    if gzfile_basename.endswith(".gz")]
        if len(gzfile_list) == 24:
            for gzfile in gzfile_list:
                try:
                    self.convert_gzfile_parquet(gzfile, spark, pagenames)
                except Exception as e:
                    error_message = f"Error while batch prrcess {gzfile} : {e}"
                    self.log_file.log_progress(error_message)
                    print(error_message)