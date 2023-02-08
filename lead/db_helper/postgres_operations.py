import psycopg2
import sys
from fastapi import HTTPException
from py4j.protocol import Py4JJavaError
from pydantic.utils import NoneType
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException, IllegalArgumentException

from lead.database_connections.DatabaseConnectionFactory import DatabaseConnectionFactory
from lead.db_helper.db_credentials_helper import get_postgres_creds
import lead.utils.constants as constants
from lead.api_helper.QueryType import QueryType


def filter_dataframe_based_on_table(source_df, postgres_df, table_name):
    if table_name == 'internet_speed':
        joined_df = source_df.join(postgres_df, on="country", how="left") \
            .select(source_df['country'],
                    source_df['avg_mbit_per_s_ookla'].alias('avg_mbit_per_s_ookla_source'),
                    postgres_df['avg_mbit_per_s_ookla'].alias('avg_mbit_per_s_ookla_pg'))

        filtered_df = joined_df.where(
            (col("avg_mbit_per_s_ookla_pg").isNull()) |
            (col("avg_mbit_per_s_ookla_pg") != col("avg_mbit_per_s_ookla_source")) |
            (col("avg_mbit_per_s_ookla_pg").isNull()) |
            (col("avg_mbit_per_s_ookla_pg") != col("avg_mbit_per_s_ookla_source"))
        ).select('country', col('avg_mbit_per_s_ookla_source').alias('avg_mbit_per_s_ookla'))

        return filtered_df
    elif table_name == 'internet_users':
        joined_df = source_df.join(postgres_df, on="id", how="left") \
            .select(source_df['id'],
                    source_df['country'].alias('country_source'),
                    postgres_df['country'].alias('country_pg'),
                    source_df['internet_users'].alias('internet_users_source'),
                    postgres_df['internet_users'].alias('internet_users_pg'),
                    source_df['population'].alias('population_source'),
                    postgres_df['population'].alias('population_pg'),
                    source_df['region'].alias('region_source'),
                    postgres_df['region'].alias('region_pg'),
                    source_df['sub_region'].alias('sub_region_source'),
                    postgres_df['sub_region'].alias('sub_region_pg'))

        filtered_df = joined_df.where(
            (col("country_pg").isNull()) |
            (col("country_pg") != col("country_source")) |
            (col("internet_users_pg").isNull()) |
            (col("internet_users_pg") != col("internet_users_source")) |
            (col("population_pg").isNull()) |
            (col("population_pg") != col("population_source")) |
            (col("region_pg").isNull()) |
            (col("region_pg") != col("region_source")) |
            (col("sub_region_pg").isNull()) |
            (col("sub_region_pg") != col("sub_region_source"))
        ).select('id',
                 col('country_source').alias('country'),
                 col('internet_users_source').alias('internet_users'),
                 col('population_source').alias('population'),
                 col('region_source').alias('region'),
                 col('sub_region_source').alias('sub_region'),
                 )

        return filtered_df
    elif table_name == 'internet_prices':
        joined_df = source_df.join(postgres_df, on="country_code", how="left") \
            .select(source_df['country_code'],
                    source_df['name'].alias('name_source'),
                    postgres_df['name'].alias('name_pg'),
                    source_df['continental_region'].alias('continental_region_source'),
                    postgres_df['continental_region'].alias('continental_region_pg'),
                    source_df['no_internal_plans'].alias('no_internal_plans_source'),
                    postgres_df['no_internal_plans'].alias('no_internal_plans_pg'),
                    source_df['avg_price_1gb_usd'].alias('avg_price_1gb_usd_source'),
                    postgres_df['avg_price_1gb_usd'].alias('avg_price_1gb_usd_pg'),
                    source_df['cheapest_1gb_30days_usd'].alias('cheapest_1gb_30days_usd_source'),
                    postgres_df['cheapest_1gb_30days_usd'].alias('cheapest_1gb_30days_usd_pg'),
                    source_df['most_expensive_1gb_usd'].alias('most_expensive_1gb_usd_source'),
                    postgres_df['most_expensive_1gb_usd'].alias('most_expensive_1gb_usd_pg'),
                    source_df['avg_1gb_start2021_usd'].alias('avg_1gb_start2021_usd_source'),
                    postgres_df['avg_1gb_start2021_usd'].alias('avg_1gb_start2021_usd_pg'),
                    source_df['avg_1gb_start2020_usd'].alias('avg_1gb_start2020_usd_source'),
                    postgres_df['avg_1gb_start2020_usd'].alias('avg_1gb_start2020_usd_pg'))

        filtered_df = joined_df.where(
            (col("name_pg").isNull()) |
            (col("name_pg") != col("name_source")) |
            (col("continental_region_pg").isNull()) |
            (col("continental_region_pg") != col("continental_region_source")) |
            (col("no_internal_plans_pg").isNull()) |
            (col("no_internal_plans_pg") != col("no_internal_plans_source")) |
            (col("avg_price_1gb_usd_pg").isNull()) |
            (col("avg_price_1gb_usd_pg") != col("avg_price_1gb_usd_source")) |
            (col("cheapest_1gb_30days_usd_pg").isNull()) |
            (col("cheapest_1gb_30days_usd_pg") != col("cheapest_1gb_30days_usd_source")) |
            (col("most_expensive_1gb_usd_pg").isNull()) |
            (col("most_expensive_1gb_usd_pg") != col("most_expensive_1gb_usd_source")) |
            (col("avg_1gb_start2021_usd_pg").isNull()) |
            (col("avg_1gb_start2021_usd_pg") != col("avg_1gb_start2021_usd_source")) |
            (col("avg_1gb_start2020_usd_pg").isNull()) |
            (col("avg_1gb_start2020_usd_pg") != col("avg_1gb_start2020_usd_source"))
        ).select('country_code',
                 col('name_source').alias('name'),
                 col('continental_region_source').alias('continental_region'),
                 col('no_internal_plans_source').alias('no_internal_plans'),
                 col('avg_price_1gb_usd_source').alias('avg_price_1gb_usd'),
                 col('cheapest_1gb_30days_usd_source').alias('cheapest_1gb_30days_usd'),
                 col('most_expensive_1gb_usd_source').alias('most_expensive_1gb_usd'),
                 col('avg_1gb_start2021_usd_source').alias('avg_1gb_start2021_usd'),
                 col('avg_1gb_start2020_usd_source').alias('avg_1gb_start2020_usd'),
                 )
        return filtered_df
    else:
        print('Wrong table name.')
        sys.exit(1)


def add_data_to_postgres_update(source_df, postgres_df, table_name):
    filtered_df = filter_dataframe_based_on_table(source_df, postgres_df, table_name)
    new_rows_count = filtered_df.count()
    if new_rows_count == 0:
        print('No new rows.')
        return
    add_data_to_postgres_overwrite(filtered_df, table_name, new_rows_count)


def add_data_to_postgres_overwrite(dataframe, table_name, new_rows_count=None):
    try:
        pg_creds = get_postgres_creds()
        dataframe.write.jdbc(
            url=f"jdbc:postgresql:{pg_creds['database']}",
            table=f"public.{table_name}",
            mode="overwrite",
            properties={
                "user": pg_creds['user'],
                "password": pg_creds['password'],
                "driver": pg_creds['driver']})

        if new_rows_count is not None:
            print('Added and/or modified rows: ', new_rows_count)

    except psycopg2.ProgrammingError:
        print("Error when executing SQL statement.")
        sys.exit(1)
    except TypeError:
        print("Expected mode argument to be a string.")
        sys.exit(1)
    except Py4JJavaError as e:
        if "SQLException" in str(e):
            print("Py4JJavaError - SQLException occurred:", e)
        elif "ConnectException" in str(e):
            print("Py4JJavaError - ConnectException occurred:", e)
        else:
            print("Py4JJavaError occurred:", e)
        sys.exit(1)
    except AnalysisException as e:
        print("AnalysisException occurred:", e)
        sys.exit(1)
    except IllegalArgumentException as e:
        print("IllegalArgumentException occurred:", e)
        sys.exit(1)
    print('Operation completed')


def query_internet_price_speed_per_country(country: str):
    if country == '':
        raise HTTPException(status_code=400, detail="Country name not provided.")

    postgres_connection = DatabaseConnectionFactory().get_connection('postgres')

    row = postgres_connection.query('one', f"SELECT country, avg_mbit_per_s_ookla, avg_price_1gb_usd"
                                           f" FROM {constants.INTERNET_PRICES} "
                                           f"INNER JOIN {constants.INTERNET_SPEED} ON {constants.INTERNET_PRICES}.name = {constants.INTERNET_SPEED}.country"
                                           f" where country='{country}'")

    if type(row) == NoneType:
        raise HTTPException(status_code=400, detail="No data found for provided parameters.")
    elif type(row) != tuple:
        raise HTTPException(status_code=500, detail="Unexpected server error.")
    elif len(row) != 3:
        raise HTTPException(status_code=400, detail='No data found for provided parameters.')
    elif type(row[0]) != str or type(row[1]) != float or type(row[2]) != float:
        raise HTTPException(status_code=500, detail="Unexpected server error.")

    print("The number of parts: ", len(row))
    return_data = {
        'country': row[0],
        'mbit_per_s': row[1],
        'price_1gb_usd': row[2]
    }
    return return_data



def query_country_nr_internet_users(query_type: str):
    if not QueryType.is_valid(query_type):
        raise HTTPException(status_code=400,
                            detail='Wrong query type param. It should be either "lowest" or "highest".')

    postgres_connection = DatabaseConnectionFactory().get_connection('postgres')

    row = None

    if query_type == 'highest':
        row = postgres_connection.query('one',
                                        f"SELECT country,internet_users from {constants.INTERNET_USERS} where internet_users = ("
                                        f"Select MAX(internet_users) from {constants.INTERNET_USERS}) LIMIT 1"
                                        )

    else:
        row = postgres_connection.query('one',
                                        f"SELECT country,internet_users from {constants.INTERNET_USERS} where internet_users = ("
                                        f"Select MIN(internet_users) from {constants.INTERNET_USERS}) LIMIT 1"
                                        )

    if type(row) != tuple:
        raise HTTPException(status_code=500, detail="Unexpected server error.")
    elif len(row) != 2:
        raise HTTPException(status_code=400, detail='No data found for provided parameters.')
    elif type(row[0]) != str or type(row[1]) != int:
        raise HTTPException(status_code=500, detail="Unexpected server error.")

    return_data = {'country': row[0], 'internet_users': row[1], 'query_type': query_type}

    return return_data


def query_region_internet_prices(query_type: str):
    if not QueryType.is_valid(query_type):
        raise HTTPException(status_code=400,
                            detail='Wrong query type param. It should be either "lowest" or "highest".')

    postgres_connection = DatabaseConnectionFactory().get_connection('postgres')

    row = None

    if query_type == 'highest':
        row = postgres_connection.query('one',
                                        f"SELECT continental_region, avg(most_expensive_1gb_usd) as avg_gb_price  "
                                        f"from {constants.INTERNET_PRICES} group by continental_region order by avg_gb_price DESC Limit 1")

    else:
        row = postgres_connection.query('one',
                                        f"SELECT continental_region, avg(cheapest_1gb_30days_usd) as avg_gb_price "
                                        f" from {constants.INTERNET_PRICES} group by continental_region order by avg_gb_price Limit 1")

    if type(row) != tuple:
        raise HTTPException(status_code=500, detail="Unexpected server error.")
    elif len(row) != 2:
        raise HTTPException(status_code=400, detail='No data found for provided parameters.')
    elif type(row[0]) != str or type(row[1]) != float:
        raise HTTPException(status_code=500, detail="Unexpected server error.")

    return_data = {
        'region': row[0],
        'price': round(row[1], 3),
        'query_type': query_type
    }

    return return_data



def query_subregion_internet_speed(query_type: str):
    if not QueryType.is_valid(query_type):
        raise HTTPException(status_code=400,
                            detail='Wrong query type param. It should be either "lowest" or "highest".')

    postgres_connection = DatabaseConnectionFactory().get_connection('postgres')

    row = None
    if query_type == 'highest':
        row = postgres_connection.query('one', f"SELECT sub_region, avg(avg_mbit_per_s_ookla) as avg_speed "
                                               f" FROM {constants.INTERNET_SPEED} "
                                               f"INNER JOIN {constants.INTERNET_USERS} ON {constants.INTERNET_SPEED}.country = {constants.INTERNET_USERS}.country"
                                               f" group by sub_region order by avg_speed DESC LIMIT 1")
    else:
        row = postgres_connection.query('one', f"SELECT2 sub_region, avg(avg_mbit_per_s_ookla) as avg_speed "
                                               f" FROM {constants.INTERNET_SPEED} "
                                               f"INNER JOIN {constants.INTERNET_USERS} ON {constants.INTERNET_SPEED}.country = {constants.INTERNET_USERS}.country"
                                               f" group by sub_region order by avg_speed ASC LIMIT 1")

    if type(row) != tuple:
        raise HTTPException(status_code=500, detail="Unexpected server error.")
    elif len(row) != 2:
        raise HTTPException(status_code=400, detail='No data found for provided parameters.')
    elif type(row[0]) != str or type(row[1]) != float:
        raise HTTPException(status_code=500, detail="Unexpected server error.")

    return_data = {
        'region': row[0],
        'internet_speed': round(row[1], 3),
        'query_type': query_type
    }

    return return_data
