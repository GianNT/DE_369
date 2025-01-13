""" Docstring
In gz_to_parquet_folder: gz_to_parquet_day
- A day has 24 files, one for each hour.

combine_for_a_day :
- Using PySpark to combine into 1 parquet file
  and then write to combine_parquet_day.

combine_for_a_month :
- Using Pyspark to combine all files (1 file presents for 1 day)
    into 1 file pretent for 1 month.

clean_gz_to_parquet_each_day_folder:
- Clean folder after combining.

"""
import os
import shutil
from calendar import monthrange
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import split, concat_ws
from log_files import LogFile


class ParquetCombine:
    """
    docstring
    """
    def __init__(self, log_file: LogFile, gz_to_parquet_folder: str) -> None:
        self.log_file = log_file
        self.gz_to_parquet_folder = gz_to_parquet_folder

    def combine_for_a_day(self, spark: SparkSession, combine_parquet_day_folder: str) -> None:
        """
        Using PySpark to combine into 1 parquet file
        and then write to combine_parquet_day.
        Parameter:
            spark : SparkSession
            combine_parquet_day_folder: the destination folder after combine 24 files into 1 files
        """

        day_name: str = list(os.listdir(self.gz_to_parquet_folder))[0].split("-")[-2]

        df_schema = StructType([
            StructField("domain", StringType()),
            StructField("pagename", StringType()),
            StructField("sumpageviewcount", IntegerType()),
            StructField("date_collection", StringType()),
            StructField("time_collection", StringType())
        ])
        try:
            df_read = spark.read.schema(df_schema).parquet(f"{self.gz_to_parquet_folder}/*")
            print(f"Number of records in DataFrame: {df_read.count()}")
            df_read.write.mode("overwrite").parquet(os.path.join(combine_parquet_day_folder, str(day_name)))
        except Exception as e:
            print(f'Error as {e}')


    def combine_for_a_month(self, spark, combine_parquet_day_folder: str, combine_parquet_month_folder):
        """
        Using PySpark to combine into 1 parquet file
        and then write to combine_parquet_day.
        Parameter:
            spark : SparkSession
            combine_parquet_day_folder: the source folder contains the daily files
            combine_parquet_month_folder: the destination folder where the combined 24 files will be written into one file
        """

        input_filename: str = list(os.listdir(combine_parquet_day_folder))[0]

        year_name: str = input_filename[:4]
        month_name: str = input_filename[4:6]
        output_filename: str = f'{year_name}-{month_name}'
        day_in_month: int = monthrange(int(year_name), int(month_name))[1]

        df_input_schema = StructType([
        StructField("domain", StringType()),
        StructField("pagename", StringType()),
        StructField("sumpageviewcount", IntegerType()),
        StructField("date_collection", StringType()),
        StructField("time_collection", StringType())
    ])

        # if len([file for file in os.listdir(combine_parquet_day_folder)]) == day_in_month:
        if len(list(os.listdir(combine_parquet_day_folder))) == day_in_month:

            try:
                df_read: DataFrame = spark.read.schema(df_input_schema).parquet(f"{combine_parquet_day_folder}/*")
                df_new: DataFrame = df_read.withColumn(
                "date_id", concat_ws("", split(df_read['date_collection'], "-")).cast(IntegerType()))\
                .withColumn("time_id", split(df_read["time_collection"],":")[0].cast(IntegerType()))\
                .withColumn("year", (split(df_read['date_collection'], "-")[0]).cast(IntegerType()))\
                .withColumn("month", (split(df_read['date_collection'], "-")[1]).cast(IntegerType()))
                df_new.write.mode("overwrite").parquet(os.path.join(combine_parquet_month_folder, str(output_filename)))
                return True
            except Exception as e:
                print(f'Error as {e}')
                return False
        else:
            # number_of_files: int = len([file for file in os.listdir(combine_parquet_day_folder)])
            number_of_files: int = len(list(os.listdir(combine_parquet_day_folder)))


            print(f'Not enough date files --- Number of files: {number_of_files} < day of month: {day_in_month}')
            return False

    def clean_gz_to_parquet_day_folder(self, processed_parquet_folder: str):
        """Move files from gz_to_parquet_day folder to processed_parquet_folder."""
        if not os.path.exists(processed_parquet_folder):
            os.makedirs(processed_parquet_folder)
            self.log_file.log_progress(f"Created folder: {processed_parquet_folder}")

        files_list = [os.path.join(self.gz_to_parquet_folder, file_basename)
                                    for file_basename in os.listdir(self.gz_to_parquet_folder)]


        for file in files_list:
            destination_path = os.path.join(processed_parquet_folder, os.path.basename(file))
            if os.path.exists(destination_path):
                if os.path.isdir(destination_path):
                    # Remove existing directory if it's a directory
                    shutil.rmtree(destination_path)
                else:
                    # Remove existing file if it's a file
                    os.remove(destination_path)
            shutil.move(file, destination_path)
            self.log_file.log_progress(f'Moved {file} to proceed parquet folder')

    def clean_combine_parquet_day_folder(self, combine_parquet_day_folder:str, processed_combine_parquet_day: str):
        """Move files from combine_parquet_day folder to processed_combine_parquet_day folder."""
        if not os.path.exists(processed_combine_parquet_day):
            os.makedirs(processed_combine_parquet_day)
            self.log_file.log_progress(f"Created folder: {processed_combine_parquet_day}")

        files_list = [os.path.join(combine_parquet_day_folder, file_basename)
                                    for file_basename in os.listdir(combine_parquet_day_folder)]


        for file in files_list:
            destination_path = os.path.join(processed_combine_parquet_day, os.path.basename(file))
            if os.path.exists(destination_path):
                if os.path.isdir(destination_path):
                    # Remove existing directory if it's a directory
                    shutil.rmtree(destination_path)
                else:
                    # Remove existing file if it's a file
                    os.remove(destination_path)
            shutil.move(file, destination_path)
            self.log_file.log_progress(f'Moved {file} to processed_parquet_folder')

# if __name__ == "__main__":

#     # Define SparkSession
#     spark = SparkSession.builder \
#     .appName("combine_parquet") \
#     .config("spark.executor.memory", "4g") \
#     .config("spark.executor.cores", "4") \
#     .config("spark.driver.memory", "4g") \
#     .getOrCreate()

#     combine: ParquetCombine = ParquetCombine()
#     input_folder = "/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/Download_to_stage/gz_to_parquet_day"
#     parquet_day = "/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/Download_to_stage/combine_parquet_day"
#     combine.combine_for_a_day(spark, input_folder, parquet_day)

#     # combine_month: ParquetCombine = ParquetCombine()
#     parquet_day = "/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/Download_to_stage/combine_parquet_day"
#     parquet_month_output = "/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/Download_to_stage/combine_parquet_year_month"
#     # combine.combine_for_a_month(spark, parquet_day, parquet_month_output)

#     if combine.combine_for_a_month(spark, parquet_day, parquet_month_output):
#         print("all done")
#     else:
#         print("step 2 break down")

#     spark.stop()
