"""  Doc string"""
from typing import TypedDict, Any
from datetime import datetime
from psycopg2.extensions import connection as Psycopg2Connection
from dim_date_table import create_dim_date_data
from dim_time_table import create_dim_time_data
import psycopg2
import pandas as pd


DateTimeFormat = datetime | str

# Define connection

class ConnectionProperty(TypedDict):
    """ doc string"""
    database: str
    user: str
    port: int
    host: str


def create_connection() -> Psycopg2Connection | None :
    """ doc string"""
    connnection_property: ConnectionProperty = {
        "database": "de_testing",
        "user": "postgres",
        "port": 5455,
        "host": "localhost"
    }

    try:
        conn = psycopg2.connect(**connnection_property)
        print("Connection sucessful!")
        return conn
    except psycopg2.Error as e:
        print(f'An error occured: {e}')
        return None


def create_table(queries: dict[str, str], connection: Psycopg2Connection):
    """ doc string"""
    cursor = connection.cursor()

    try:
        for k,v in queries.items():
            cursor.execute(v)
            connection.commit()
            print(f"The {k} run sucessful!")
    except psycopg2.Error as e:
        print(f'An error occured: {e}')
        connection.rollback()
    finally:
        cursor.close()


def load_data_to_table(connection: Psycopg2Connection,\
                        query: str, values: list[tuple[Any,...]]) -> None:
    """ doc string """
    cursor = connection.cursor()
    try:
        cursor.executemany(query, values)
        connection.commit()
        print("Data load sucessfull!")
    except psycopg2.Error as e:
        print(f'An error occured: {e}')
        connection.rollback()


if __name__ == "__main__":
# Dim_date creation
    # Create dim_date dataframe
    start_date: DateTimeFormat = datetime(2015,1,1)
    end_date: DateTimeFormat = datetime(2025,12,31)
    df_dim_date = create_dim_date_data(start_date, end_date)

    # Create dim_date table queries
    DROP_TABLE_QUERY = "drop table if exists dim_date CASCADE;"
    CREATE_TABLE_QUERY = """create table if not exists dim_date\
                    (
                        date_id bigint,\
                        "date" date,\
                        year int, \
                        quarter int,\
                        month int,\
                        month_name varchar(20),\
                        week_of_year int,\
                        day int,\
                        day_name varchar(10),\
                        is_weekend bool,\
                        constraint pk_date_id PRIMARY KEY (date_id)
                        );"""
    dim_date_queries: dict[str, str] = {"drop_dim_date_table_query": DROP_TABLE_QUERY,
                                        "create_dim_date_table_query" :CREATE_TABLE_QUERY}

    conn_create = create_connection()
    if conn_create:
        create_table(queries= dim_date_queries, connection= conn_create)
        conn_create.close()

    # Load data to dim_date table
    conn_load = create_connection()
    if conn_load:
        VALUE = [tuple(row) for row in df_dim_date.values]
        # LOAD_DATA_TO_TABLE_QUERY = """
        #                 insert into dim_date({", ".join([f'{i}' for i in (df_dim_date.columns)])})\
        #                 values (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s)
        #             """
        LOAD_DATA_TO_TABLE_QUERY = \
                         f"insert into dim_date({", ".join([f'{i}' for i in (df_dim_date.columns)])}) values ({" ,".join((len(df_dim_date.columns)*['%s']))})"

        print(LOAD_DATA_TO_TABLE_QUERY)

        load_data_to_table(connection=conn_load, query= LOAD_DATA_TO_TABLE_QUERY, values= VALUE)
        conn_load.close()

    # Dim_time creation
    df_dim_time = create_dim_time_data(pd.Series(range(0,24)))

    #Create dim_time table queries
    DROP_TABLE_QUERY = "drop table if exists dim_time CASCADE;"
    CREATE_TABLE_QUERY = """create table if not exists dim_time(
                            time_id int,
                            time_creation TIME,
                            CONSTRAINT pk_time_id PRIMARY KEY (time_id)
                            );"""
    dim_time_queries: dict[str, str] = {"drop_dim_time_table_query": DROP_TABLE_QUERY,
                                        "create_dim_time_table_query" :CREATE_TABLE_QUERY}

    conn_create = create_connection()
    if conn_create:
        create_table(queries= dim_time_queries, connection= conn_create)
        conn_create.close()

    # Load data to dim_time table
    conn_load = create_connection()
    if conn_load:
        VALUE = [tuple(row) for row in df_dim_time.values]
        # LOAD_DATA_TO_TABLE_QUERY = """
        #                 insert into dim_date({", ".join([f'{i}' for i in (df_dim_date.columns)])})\
        #                 values (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s)
        #             """
        LOAD_DATA_TO_TABLE_QUERY = \
                         f"insert into dim_time({", ".join([f'{i}' for i in (df_dim_time.columns)])}) values ({" ,".join((len(df_dim_time.columns)*['%s']))})"

        print(LOAD_DATA_TO_TABLE_QUERY)

        load_data_to_table(connection=conn_load, query= LOAD_DATA_TO_TABLE_QUERY, values= VALUE)
        conn_load.close()


# Fact pageview creation

DROP_TABLE_QUERY = "drop table if exists fact_pageviews cascade;"
CREATE_TABLE_QUERY = """CREATE TABLE fact_pageviews (
                            fact_id BIGSERIAL,   -- surrogate key for unique identification
                            date_id INT NOT NULL,
                            time_id INT NOT NULL,
                            domain VARCHAR(30) NOT NULL,
                            pagename VARCHAR(30) NOT NULL,
                            sumpageviewcount BIGINT NOT NULL,
                            year INT NOT NULL,
                            month INT NOT NULL,
                                CONSTRAINT pk_fact_pageviews PRIMARY KEY (fact_id, year, month),
                            CONSTRAINT fk_fact_pageviews_date FOREIGN KEY (date_id) REFERENCES dim_date (date_id),
                            CONSTRAINT fk_fact_pageviews_time FOREIGN KEY (time_id) REFERENCES dim_time (time_id)
                        )
                        PARTITION BY RANGE (year, month);"""
CREATE_PARTITION_TABLE_QUERY = """
                                    DO
                                    $$
                                    DECLARE
                                        y INT := 2015; -- starting YEAR
                                        m INT := 1; 		-- starting month
                                        max_year INT := 2030;
                                    BEGIN
                                        while y <= max_year LOOP
                                            WHILE m <= 12 LOOP
                                                EXECUTE
                                                    format('create table fact_pageviews_y%sm%s PARTITION OF fact_pageviews
                                                    for values from (%s, %s) to (%s, %s);',
                                                    y, LPAD(m::TEXT, 2, '0'),
                                                    y, m,
                                                    y + (m/12)::INT, case when m =12 then 1 else m+1 END);
                                                    m:= m+1;
                                            END LOOP;
                                            m := 1;
                                            y := y+1;
                                        END LOOP;
                                    END $$;
                                    """
fact_pageviews_queries: dict[str, str] = {"drop_dim_time_table_query": DROP_TABLE_QUERY,
                                    "create_dim_time_table_query" :CREATE_TABLE_QUERY,
                                    "create_partition_tables":CREATE_PARTITION_TABLE_QUERY}

conn_create = create_connection()
if conn_create:
    create_table(queries= fact_pageviews_queries, connection= conn_create)
    conn_create.close()
    print("")
