""" Docs String
Download Gz files from web with URL.
"""
# Import necessary libraries
from datetime import datetime
import gzip
import os
import subprocess
from typing import Optional
from Modules.log_files import LogFile


# Generate URL funcion
class GzProcessor:
    """
    A class to handle download GZ file from web
    URL like: https://dumps.wikimedia.org/other/pageviews/2015/2015-05/pageviews-20150501-010000.gz
    Method:
        create_url : create URL to download with parameter: year, month, day
        download_gzfile : Download gz file, retry to download again if not valid gz file.
        is_valid_gz : Check download gz file is valid
        download_gzfiles_eachday : Download batch for each day. \
                                   a day has 24 gz files, one for 24 hours
        valid_gzfolder : Recheck to ensure there are 24 gz files for each day.
        clean_gz_raw_folder: Move files to another folder to saving.
    """
    def __init__(self, gzfolder: str,log_file: LogFile, max_retries: int = 3) -> None:
        self.gzfolder = gzfolder
        self.log_file = log_file
        self.max_retries = max_retries

    def create_url(self, year: int, month: int, date: int, time: int) -> str:
        """
        Create URL:
        Parameter:
            year : int, format 'yyyy'
            month: int, format 'mm'
            date : int, format 'dd'
            time : int, format 'HH'
        """
        try:
            datetime(year, month, date)
            url = f'https://dumps.wikimedia.org/other/pageviews/{year:04}/{year:04}-{month:02}/pageviews-{year:04}{month:02}{date:02}-{time:02}0000.gz'
            return url
        except ValueError as e:
            error_message = f'Error occured: {e}'
            self.log_file.log_progress(error_message)
            print(error_message)
            raise

    def is_valid_gz(self, gzfile: str) -> bool:
        """
        Check download gz file is valid
        Paremeter:
            gzfile : str, The gzfile will be checked
        Returns:
            bool: True if the file is a valid gz file, False otherwise.
        """
        try:
            with gzip.open(gzfile, 'rb') as gz_check:
                gz_check.read(1)
            return True
        except (gzip.BadGzipFile, OSError):
            self.log_file.log_progress(f'Invalid gzip file: {gzfile}')
            return False

    def download_gzfile(self, url: str) -> Optional[str]:
        """ download gz file from URL, recheck if validated gz file, retry if not"""
        gzfile_basename = url.split("/")[-1]
        gzfile = os.path.join(self.gzfolder,gzfile_basename)

        # Check if the file already exists
        if os.path.exists(gzfile):
            inform_message =f'{gzfile} already exsits. Skipping download.'
            self.log_file.log_progress(inform_message)
            print(inform_message)

            if self.is_valid_gz(gzfile):
                return gzfile

            print(f'{gzfile} is invalid. Removing...')
            os.remove(gzfile)

        # Retry logic for downloading
        for attempt in range(self.max_retries):
            try:
                self.log_file.log_progress(f"Attempting to download: {gzfile} (Attempt {attempt + 1})")
                print(f"Downloading {gzfile}...")
                subprocess.run(["curl", "-o", gzfile, url], check=True)

                if self.is_valid_gz(gzfile):
                    self.log_file.log_progress(f"Successfully validated: {gzfile_basename}")
                    print(f"Validated: {gzfile_basename}")
                    return gzfile

                self.log_file.log_progress(f"Invalid file: {gzfile_basename}. Removing...")
                print(f"Invalid file: {gzfile_basename}. Removing...")
                os.remove(gzfile)

            except subprocess.CalledProcessError as e:
                error_message = f'Error downloading {url} : {e}'
                self.log_file.log_progress(error_message)
                print(error_message)

        # Return None if all attempts fail
        self.log_file.log_progress(f"Failed to download {gzfile_basename} after {self.max_retries} attempts.")
        print(f"Failed to download {gzfile_basename} after {self.max_retries} attempts.")
        return None

    def download_gzfiles_eachday(self, year: int, month: int, date: int) -> None:
        """ Download gz files for each hour of the specified day. """
        for hour in range(24):
            url = self.create_url(year, month, date, hour)

            if not url:
                error_message = f"Invalid URL generated for {year}-{month}-{date} at hour {hour}. Skipping."
                self.log_file.log_progress(error_message)
                continue

            try:
                self.log_file.log_progress(f"Attempting to download: {url}")
                self.download_gzfile(url)

            except Exception as e:
                error_message = f"Unable to download valid file: {e}"
                self.log_file.log_progress(error_message)

    def valid_gzfolder(self) -> bool:
        """ Ensure the folder contains 24 valid .gz files"""
        gzfile_list: list[str] = [os.path.join(self.gzfolder, gzfile_basename_check)
                       for gzfile_basename_check in os.listdir(self.gzfolder) if gzfile_basename_check.endswith('.gz')]

        if len(gzfile_list) == 24:
            # check ivalid files
            invalid_gzfiles: list[str] = [gzfile for gzfile in gzfile_list if not self.is_valid_gz(gzfile)]
            if invalid_gzfiles:
                for invalid_file in invalid_gzfiles:
                    inform_message = f'Removing invalid file {invalid_file}'
                    self.log_file.log_progress(inform_message)
                    print(inform_message)
                    os.remove(invalid_file)

                for invalid_file in invalid_gzfiles:
                    self.download_gzfile(invalid_file)

                return self.valid_gzfolder()
            return True

        inform_message = f'Expected 24 .gz files, found {len(gzfile_list)}'
        self.log_file.log_progress(inform_message)
        print(inform_message)
        return False

    def clean_gz_raw_folder(self) -> None:
        """ clean folder after converting, combining 24 files to 1 day parquet file"""
        gzfile_list: list[str] = [os.path.join(self.gzfolder, gzfile_basename_check)
                       for gzfile_basename_check in os.listdir(self.gzfolder) if gzfile_basename_check.endswith('.gz')]
        for gzfile in gzfile_list:
            os.remove(gzfile)


# if __name__ == "__main__":
#     log_file = LogFile("/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/logfile.txt")
#     log_file.reset_logfile()

#     # gzfolder_path = "/Users/GianNT/Documents/DE_project_venv/Project_1_Wiki_DW/Download_to_stage/gz_test"
#     gzfolder_path ="/Users/GianNT/Documents/DE_project_venv/new_copy_check/sample_gz_files"
#     downloader: GzProcessor = GzProcessor(gzfolder_path,log_file, 3)
#     year, month, days = 2015, 9, [1]
#     for day in days:
#         try:
#             downloader.download_gzfiles_eachday(year, month, day)
#             print("All files download sucessfully")
#         except Exception as e:
#             print(f'Batch download failed{e}')

#     downloader.valid_gzfolder()
