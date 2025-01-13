""" Create Dim_date table """
from datetime import datetime
from annotated_types import UpperCase
import pandas as pd
from pandas import DatetimeIndex


DateTimeFormat = datetime | str

def create_dim_date_data(start_date: DateTimeFormat, end_date: DateTimeFormat) -> pd.DataFrame:
    """
    Create DataFrame containing date-related dimension

    Parameter:
        start_date, end_date : Datetime format
        -> DatetimeIndex (create with start_date, end_date by pd.date_range)

    Returns: DataFrame
        'date_id'      : int
        'date'         : datetime (pandas now treat dates: DatetimeIndex as column)
        'year'         : int
        'quarter'      : int
        'month'        : int
        'month_name'   : str
        'week_of_year' : int
        'day'          : int
        'day_name'     : str
        'is_weekend'   : bool
    """

    dates: DatetimeIndex = pd.date_range(start_date, end_date)
    df_dates = pd.DataFrame(
        {
            'date_id': dates.strftime("%Y%m%d").astype(int),
            'date': dates,
            'year': getattr(dates,"year"),
            'quarter': getattr(dates, "quarter"),
            'month': getattr(dates, "month"),
            'month_name': dates.strftime('%B'),
            'week_of_year': dates.strftime('%U').astype(int),
            'day': getattr(dates, "day"),
            'day_name': dates.strftime('%A'),
            'is_weekend': (getattr(dates, "weekday") > 5)
        }
    )
    return df_dates

# res = create_dim_date_data(datetime(2015,1,1), datetime(2015,1,20))
# print(res.dtypes)
