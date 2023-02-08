import json
from py4j.protocol import Py4JJavaError
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.utils import AnalysisException, IllegalArgumentException

from lead.db_helper.db_credentials_helper import get_postgres_creds
from lead.db_helper.postgres_operations import add_data_to_postgres_update, add_data_to_postgres_overwrite
from lead.api_helper.datasource_rest_api import login_rest_api
from lead.api_helper.datasource_rest_api import get_api_data
import lead.utils.constants as constants
from lead.api_helper.InsertMode import InsertMode

JWT_token = login_rest_api()

if JWT_token:
    auth_header = {
        'authorization': 'bearer ' + JWT_token
    }
    rest_api_data = get_api_data(auth_header)

    if rest_api_data:
        if not hasattr(constants, 'INTERNET_SPEED_HEADERS'):
            print("Could not retrieve headers file")
            sys.exit(1)
        elif not hasattr(constants, 'INTERNET_SPEED'):
            print("Could not retrieve table name.")
            sys.exit(1)

        with open(constants.INTERNET_SPEED_HEADERS, 'r') as file:
            headers_map = json.load(file)
        if headers_map is None:
            print("Could not get api data headers.")
            sys.exit(1)

        headers = list(headers_map.values())

        spark = SparkSession.builder \
            .appName("Internet Speed") \
            .getOrCreate()

        # prepare rest api data to be inserted in a dataframe
        rest_api_data = [tuple(el.split(','))
                         for el in rest_api_data[1:]
                         if len(el.split(',')) == 9]

        rest_api_data_df = spark.createDataFrame(data=rest_api_data, schema=headers)

        # delete rows that dont have country or region or country code
        rest_api_data_df = rest_api_data_df.filter(
            (rest_api_data_df.name != '') &
            (rest_api_data_df.continental_region != '') &
            (rest_api_data_df.country_code != '')
        )

        rest_api_data_df = rest_api_data_df.dropDuplicates(subset=['country_code'])
        rest_api_data_df = rest_api_data_df.dropDuplicates(subset=['name'])

        # replacing \n and $ character and casting the columns to double
        rest_api_data_df = rest_api_data_df \
            .withColumn("no_internal_plans",
                        func.col("no_internal_plans").cast("int")) \
            .withColumn('avg_1gb_start2020_usd',
                        func.regexp_replace('avg_1gb_start2020_usd', '\n', '')) \
            .withColumn('avg_price_1gb_usd',
                        func.regexp_replace('avg_price_1gb_usd', '\\$', '')) \
            .withColumn('avg_price_1gb_usd',
                        func.col("avg_price_1gb_usd").cast("double")) \
            .withColumn('cheapest_1gb_30days_usd',
                        func.regexp_replace('cheapest_1gb_30days_usd', '\\$', '')) \
            .withColumn('cheapest_1gb_30days_usd',
                        func.col("cheapest_1gb_30days_usd").cast("double")) \
            .withColumn('most_expensive_1gb_usd',
                        func.regexp_replace('most_expensive_1gb_usd', '\\$', '')) \
            .withColumn('most_expensive_1gb_usd',
                        func.col("most_expensive_1gb_usd").cast("double")) \
            .withColumn('avg_1gb_start2021_usd',
                        func.regexp_replace('avg_1gb_start2021_usd', '\\$', '')) \
            .withColumn('avg_1gb_start2021_usd',
                        func.col("avg_1gb_start2021_usd").cast("double")) \
            .withColumn('avg_1gb_start2020_usd',
                        func.regexp_replace('avg_1gb_start2020_usd', '\\$', '')) \
            .withColumn('avg_1gb_start2020_usd',
                        func.col("avg_1gb_start2020_usd").cast("double"))

        # creating a df that contains the average values for every region
        average_per_region = rest_api_data_df.groupBy(func.col('continental_region')) \
            .agg(func.round(func.avg('no_internal_plans'), 2).alias("avg_no_internal_plans"),
                 func.round(func.avg('avg_price_1gb_usd'), 2).alias("avg_avg_price_1gb_usd"),
                 func.round(func.avg('cheapest_1gb_30days_usd'), 2).alias("avg_cheapest_1gb_30days_usd"),
                 func.round(func.avg('avg_1gb_start2021_usd'), 2).alias("avg_avg_1gb_start2021_usd"),
                 func.round(func.avg('avg_1gb_start2020_usd'), 2).alias("avg_avg_1gb_start2020_usd"),
                 func.round(func.avg('most_expensive_1gb_usd'), 2).alias("avg_most_expensive_1gb_usd"))

        # join the normal and average dataframes
        # in order to replace the null values with their regional averages
        rest_api_data_df = rest_api_data_df.join(average_per_region, "continental_region", "left")

        rest_api_data_df = rest_api_data_df \
            .withColumn('no_internal_plans',
                        func.when(func.col('no_internal_plans').isNull(),
                                  func.col('avg_no_internal_plans'))
                        .otherwise(func.col('no_internal_plans'))) \
            .withColumn('avg_price_1gb_usd',
                        func.when(func.col('avg_price_1gb_usd').isNull(),
                                  func.col('avg_avg_price_1gb_usd')).otherwise(
                            func.col('avg_price_1gb_usd'))) \
            .withColumn('cheapest_1gb_30days_usd',
                        func.when(func.col('cheapest_1gb_30days_usd').isNull(),
                                  func.col('avg_cheapest_1gb_30days_usd')).otherwise(
                            func.col('cheapest_1gb_30days_usd'))) \
            .withColumn('avg_1gb_start2021_usd',
                        func.when(func.col('avg_1gb_start2021_usd').isNull(),
                                  func.col('avg_avg_1gb_start2021_usd')).otherwise(
                            func.col('avg_1gb_start2021_usd'))) \
            .withColumn('avg_1gb_start2020_usd',
                        func.when(func.col('avg_1gb_start2020_usd').isNull(),
                                  func.col('avg_avg_1gb_start2020_usd')).otherwise(
                            func.col('avg_1gb_start2020_usd'))) \
            .withColumn('most_expensive_1gb_usd',
                        func.when(func.col('most_expensive_1gb_usd').isNull(),
                                  func.col('avg_most_expensive_1gb_usd')).otherwise(
                            func.col('most_expensive_1gb_usd')))

        # keeping only the required columns
        rest_api_data_df = rest_api_data_df.select('country_code', 'name', 'continental_region', 'no_internal_plans',
                                                   'avg_price_1gb_usd', 'cheapest_1gb_30days_usd',
                                                   'most_expensive_1gb_usd', 'avg_1gb_start2021_usd',
                                                   'avg_1gb_start2020_usd')
        rest_api_data_df = rest_api_data_df.dropna()

        try:
            insert_mode = sys.argv[1]
            if insert_mode == InsertMode.UPDATE.value:
                pg_creds = get_postgres_creds()
                target_df = spark.read.jdbc(
                    url=f"jdbc:postgresql:{pg_creds['database']}",
                    table=f"public.{constants.INTERNET_PRICES}",
                    properties={
                        "user": pg_creds['user'],
                        "password": pg_creds['password'],
                        "driver": pg_creds['driver']})
                add_data_to_postgres_update(rest_api_data_df, target_df, constants.INTERNET_PRICES)

            elif insert_mode == InsertMode.OVERWRITE.value:
                add_data_to_postgres_overwrite(rest_api_data_df, constants.INTERNET_PRICES)
            else:
                print('Bad value for insert_mode argument.')
                sys.exit(1)
        except IndexError:
            print('Value not found in system arguments.')
            sys.exit(1)
        except AttributeError:
            print('Could not find required constant values.')
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

        spark.stop()
