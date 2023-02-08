import pyspark.sql.functions as func
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
import sys

from pyspark.sql.utils import AnalysisException, IllegalArgumentException

from lead.db_helper.db_credentials_helper import get_mongo_string_creds, get_postgres_creds
from lead.db_helper.postgres_operations import add_data_to_postgres_overwrite, add_data_to_postgres_update
import lead.utils.constants as constants
from lead.api_helper.InsertMode import InsertMode

if not hasattr(constants, 'INTERNET_USERS'):
    print('Could not retrieve constant values.')
    sys.exit(1)

mongo_str = get_mongo_string_creds()
spark = SparkSession.builder.appName("mongospark").config("spark.mongodb.input.uri", mongo_str) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()

# load data from mongo
mongo_data_df = spark.read \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .load()

# _id is a struct type that contains oid, we will transform it to a simple ID string column
mongo_data_df = mongo_data_df.select('*', func.col('_id.oid').alias('id')).drop('_id')

# drop rows that dont have id, region, sub region or country
mongo_data_df = mongo_data_df.filter(
    (~func.col('id').isNull()) & (func.col('region') != '') & (func.col('sub_region') != '') & (
            func.col('country') != ''))

# drop duplicates for id or country
mongo_data_df = mongo_data_df.dropDuplicates(subset=['id']).dropDuplicates(subset=['country'])

# replace commas and cast to integer
# will first cast to double and then integer, to avoid getting the scientific form of the numbers
mongo_data_df = mongo_data_df \
    .withColumn('internet_users',
                func.regexp_replace('internet_users', ',', '')) \
    .withColumn("internet_users",
                func.col("internet_users").cast('double').cast('int')) \
    .withColumn('population',
                func.regexp_replace('population', ',', '')) \
    .withColumn("population",
                func.col("population").cast('double').cast('int'))

# create average table per region
average_per_region = mongo_data_df.groupBy(func.col('region')) \
    .agg(func.round(func.avg('internet_users'), 0).cast('double').cast('int').alias("avg_internet_users"),
         func.round(func.avg('population'), 0).cast('double').cast('int').alias("avg_population"))

# join tables and fill null values
mongo_data_df = mongo_data_df.join(average_per_region, "region", "left")
mongo_data_df = mongo_data_df \
    .withColumn('internet_users',
                func.when(func.col('internet_users').isNull(),
                          func.col('avg_internet_users'))
                .otherwise(func.col('internet_users'))) \
    .withColumn('population',
                func.when(func.col('population').isNull(),
                          func.col('avg_population')).otherwise(
                    func.col('population')))

# keep only the original columns
mongo_data_df = mongo_data_df.select('id', 'country', 'internet_users', 'population', 'region', 'sub_region')

try:
    insert_mode = sys.argv[1]
    if insert_mode == InsertMode.UPDATE.value:
        pg_creds = get_postgres_creds()
        target_df = spark.read.jdbc(
            url=f"jdbc:postgresql:{pg_creds['database']}",
            table=f"public.{constants.INTERNET_USERS}",
            properties={
                "user": pg_creds['user'],
                "password": pg_creds['password'],
                "driver": pg_creds['driver']})
        add_data_to_postgres_update(mongo_data_df, target_df, constants.INTERNET_USERS)

    elif insert_mode == InsertMode.OVERWRITE.value:
        add_data_to_postgres_overwrite(mongo_data_df, constants.INTERNET_USERS)
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
