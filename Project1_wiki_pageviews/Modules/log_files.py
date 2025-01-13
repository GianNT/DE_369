""" create log file to catch events"""

from datetime import datetime

class LogFile:
    """ This class provides functionality to log messages with timestamps and
    to reset the log file's contents."""
    def __init__(self, file_name: str) -> None:
        """  Initializes a LogFile instance. """
        self.file_name = file_name

    def log_progress(self, message: str) -> None:
        """
        Each message is appended to the log file, prefixed with the current
        date and time in the format 'YYYY-MM-DD HH:MM:SS'
        """
        now_time: datetime = datetime.now()
        event_time: str = now_time.strftime("%Y-%m-%d %H:%M:%S")
        with open(self.file_name, 'a', encoding= 'utf-8') as log_writer:
            log_writer.write(f'{event_time} : {message}\n')

    def reset_logfile(self) -> None:
        """ Clears the contents of the log file. """
        with open(self.file_name, 'w+', encoding='utf-8') as log_reset:
            log_reset.write("")
