import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'Modules')))
from Modules.download_gz_from_web import GzProcessor
from Modules.gz_to_parquet import GzToParquet
from Modules.log_files import LogFile
from Modules.parquet_combine import ParquetCombine
from Modules.load_to_dw import LoadToDW
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType
import gc

def create_spark_session():
    """Create and return a single Spark session for both tasks."""
    return SparkSession.builder \
        .appName("My_app") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.jars", "/opt/homebrew/Cellar/apache-spark/3.5.3/libexec/bin/postgresql-42.7.3.jar") \
        .getOrCreate()

def download_transform_combine():
    ### DOWNLOAD - TRANSFORM - COMBINE
    log_file = LogFile("/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/logfile.txt")
    log_file.reset_logfile()

    # Define download from web to stage raw
    gz_raw_folder = "/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/Download_to_stage/gz_raw"
    downloader = GzProcessor(gz_raw_folder,log_file, 3)


    # Define transform gz files to parquet files
    spark = create_spark_session()

    pagenames_request = ["Google","Facebook","Amazon","Microsoft", "Apple", 'Walmart']
    gz_to_parquet_folder = "/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/Download_to_stage/gz_to_parquet_day"
    transform = GzToParquet(log_file, gz_raw_folder, gz_to_parquet_folder)

    # Define combine parquets files each day, each month
    combine = ParquetCombine(log_file, gz_to_parquet_folder)
    gz_to_parquet_folder = "/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/Download_to_stage/gz_to_parquet_day"
    processed_parquet_folder = "/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/Download_to_stage/processed_parquet_folder"

    combine_parquet_day_folder = "/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/Download_to_stage/combine_parquet_day"
    combine_parquet_month_folder = "/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/Download_to_stage/combine_parquet_year_month"

    processed_combine_parquet_day = "/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/Download_to_stage/processed_combine_parquet_day"

    # Run flow
    try:
        year, month, days = 2015, 9, [2]
        for day in days:
            downloader.download_gzfiles_eachday(year, month, day)
            print("All GZ files download sucessfully")
            if downloader.valid_gzfolder():
                transform.convert_gzfiles_day(spark, pagenames_request)
                combine.combine_for_a_day(spark, combine_parquet_day_folder)
                downloader.clean_gz_raw_folder()
                combine.clean_gz_to_parquet_day_folder(processed_parquet_folder)

                if combine.combine_for_a_month(spark, combine_parquet_day_folder, combine_parquet_month_folder):
                    print(f"Combined {year}-{month} file.")
                    combine.clean_combine_parquet_day_folder(combine_parquet_day_folder, processed_combine_parquet_day)
                else:
                    print("step 2 break down")
                gc.collect()
    finally:
        spark.stop()


def load_to_dw():
    ### LOAD to DW
    load_data_logfile_test = LogFile("/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/load_data_log.txt")
    combine_parquet_year_month = "/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/Download_to_stage/combine_parquet_year_month"
    processed_load_to_WD = "/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/Download_to_stage/processed_load_to_wh"

    spark_load = create_spark_session()

    fixed_schema = StructType([
    StructField("domain", StringType()),
    StructField("pagename", StringType()),
    StructField("sumpageviewcount", IntegerType()),
    StructField("date_collection", StringType()),
    StructField("time_collection", StringType()),
    StructField("date_id", IntegerType()),
    StructField("time_id", IntegerType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType())
])
    DATABASE_NAME = 'de_testing'
    load_to_pagevies = LoadToDW(load_data_logfile_test, spark_load, combine_parquet_year_month)
    try:
        if load_to_pagevies.load_to_dw(schema= fixed_schema, database= DATABASE_NAME):
            load_to_pagevies.save_processed_file(processed_load_to_WD)
            print("loaded to dw")
    finally:
        spark_load.stop()

if __name__ == "__main__":

    download_transform_combine()
    load_to_dw()
