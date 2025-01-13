"""
Load data from parquet files/ folder to fact_pageviews in Data Warehouse with PySpark
"""
# Import libraries
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from log_files import LogFile


class LoadToDW:
    """
    A class to handle loading data into a data warehouse (DW) from processed Parquet files.
    Attributes:
        load_data_logfile (LogFile): Logger to track the progress \
                                     and errors during the data loading process.
        spark (SparkSession)
        combine_parquet_year_month (str)
    Methods:
        create_df_to_load(schema: StructType) -> DataFrame | None:
            Creates and prepares a Spark DataFrame for loading into the data warehouse,
            partitioned by year and month and sorted by date_id and time_id.

        load_to_DW(schema: StructType, database: str) -> bool:
            Loads the prepared DataFrame into the specified database \
            and returns whether the operation was successful.

        save_processed_file(processed_folder: str) -> None:
            Moves Parquet files after successful loading into the data warehouse.
    """
    def __init__(self, load_data_logfile: LogFile,
                 spark: SparkSession,
                 combine_parquet_year_month: str
                 ):
        """
        Initialize the LoadToDW instance.
        Parameters:
            load_data_logfile: LogFile instance for logging progress and errors.
            spark: SparkSession instance for interacting with Spark.
            combine_parquet_year_month: Path to the folder containing the Parquet files.
        """
        self.load_data_logfile = load_data_logfile
        self.spark = spark
        self.combine_parquet_year_month = combine_parquet_year_month

    def create_df_to_load(self, schema) -> DataFrame | None:
        """ This method create DataFrame which sorted with Partition data_id, time_id
            in Partition by month.
            Parameter:
                schema: DataFrame schema
            Return:
                DataFrame
        """
        try:
            df = self.spark.read.schema(schema).parquet(f'{self.combine_parquet_year_month}/*')
            row_to_write = ["date_id","time_id", "domain", "pagename",\
                             "sumpageviewcount", "year", "month"
                            ]
            df_to_write = df.select(*row_to_write)
            df_partitions = df_to_write.repartition('month')
            df_sorted= df_partitions.sortWithinPartitions('date_id', "time_id")
            return df_sorted
        except IOError as ioe:
            error_message = f'Error occured: {ioe}'
            self.load_data_logfile.log_progress(error_message)
            return None

    def load_to_dw(self, schema, database: str, table_name: str = 'fact_pageviews'):
        """ This method to load DataFrame created in method create_df_to_load to Database
            Parameters:
                schema     : DataFrame schema
                database   : database name
                table_name : destination table name in data base.
        """
        try:
            df = self.create_df_to_load(schema)
            if df:
                # Define posgresql url
                postgresql_url = f"jdbc:postgresql://localhost:5455/{database}"
                # Define connection_properties
                connection_properties = {
                    "user":"postgres",\
                    "driver":"org.postgresql.Driver",\
                    "batchsize":"10000"
                    }
                # Write to database
                df.write.jdbc(
                                url = postgresql_url,
                                table = table_name,
                                mode = 'append',
                                properties = connection_properties
                            )
                self.load_data_logfile.log_progress("Data loaded sucessfully!")
                print("Data loaded sucessfully!")
                return True
        except IOError as e:
            self.load_data_logfile.log_progress(f"Error loading data to DW: {str(e)}")
            print(f"error as {e}")
        except ValueError as ve:
            self.load_data_logfile.log_progress(f"ValueError: {ve}")
            print(f"error as {ve}")
        return False

    def save_processed_file(self, processed_load_to_dw: str) -> None:
        """ Move files from combine_parquet_year_month folder
            to processed_load_to_dw folder after loading data to database.
            Parameters:
                processed_load_to_dw : path to processed_load_to_dw folder

        """
        if not os.path.exists(processed_load_to_dw):
            os.makedirs(processed_load_to_dw)
            self.load_data_logfile.log_progress(f"Created folder: {processed_load_to_dw}")

        files_list = [os.path.join(self.combine_parquet_year_month, file_basename)
                                    for file_basename
                                    in os.listdir(self.combine_parquet_year_month)]
        for file in files_list:
            destination_path = os.path.join(processed_load_to_dw, os.path.basename(file))
            if os.path.exists(destination_path):
                if os.path.isdir(destination_path):
                    # Remove existing directory if it's a directory
                    shutil.rmtree(destination_path)
                else:
                    # Remove existing file if it's a file
                    os.remove(destination_path)
            shutil.move(file, destination_path)
            self.load_data_logfile.log_progress(f'Moved {os.path.basename(file)}\
                                                    to {processed_load_to_dw}')
