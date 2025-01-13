""" Create dim_time_table_data
    2 columns:
    time_id: int = range(0,24)
    time_creation: str with format: '00:00:00'
"""

import pandas as pd

def create_dim_time_data(time_index: pd.Series | list[str] | range) -> pd.DataFrame:
    """
    Create DataFrame containing time-related dimension

    Parameter:
        time_index : A sequence of hour value (0-23)
        It can be Pandas Series, list, or range of integers present hours.
    Returns:
        pd.Dataframe: A frame with 2 columns:
            'time_id'       : int, unique
            'time_createion : coressponding datetime.time object for each hour
    """

    # Convert input to pandas Series
    if isinstance(time_index, (list, range)):
        time_index_series = pd.Series(time_index)
    elif isinstance(time_index, pd.Series):
        time_index_series = time_index
    else:
        raise ValueError("time_index must be a pandas Series, list, or range of integers.")

    #validate the input
    if not time_index_series.apply(lambda x: isinstance(x, int)).all():
        raise ValueError("time_index must contain integers of strings present hours.")
    if not all(time_index_series.between(0, 23)):
        raise ValueError("All hour value in time_index must be between 0 and 24")

    #create DataFrame
    df_times = pd.DataFrame({
        'time_id': time_index_series,
        'time_creation': pd.to_datetime(time_index_series, format= "%H").dt.time
        })

    return df_times
