from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
import sys

from pyspark.sql.utils import AnalysisException, IllegalArgumentException

from lead.db_helper.db_credentials_helper import get_postgres_creds
from lead.db_helper.postgres_operations import add_data_to_postgres_update, add_data_to_postgres_overwrite
import lead.utils.constants as constants
from lead.api_helper.InsertMode import InsertMode

spark = SparkSession.builder \
    .appName("Internet Speed") \
    .getOrCreate()
if not hasattr(constants, 'DS_LOCAL_FILE'):
    print('Could not retrieve local file location.')
    sys.exit(1)
elif not hasattr(constants, 'INTERNET_SPEED'):
    print('Could not retrieve table name.')
    sys.exit(1)

local_df = spark.read.csv(constants.DS_LOCAL_FILE, header=True, inferSchema=True)

local_df = local_df \
    .withColumnRenamed('Country', 'country') \
    .withColumnRenamed('Avg(Mbit/s)Ookla', 'avg_mbit_per_s_ookla')

# drop duplicates of country and any row that contains null values
local_df = local_df.dropDuplicates(subset=['country'])
local_df = local_df.dropna()

try:
    insert_mode = sys.argv[1]
    if insert_mode == InsertMode.UPDATE.value:
        pg_creds = get_postgres_creds()
        target_df = spark.read.jdbc(
            url=f"jdbc:postgresql:{pg_creds['database']}",
            table=f"public.{constants.INTERNET_SPEED}",
            properties={
                "user": pg_creds['user'],
                "password": pg_creds['password'],
                "driver": pg_creds['driver']})
        add_data_to_postgres_update(local_df, target_df, constants.INTERNET_SPEED)

    elif insert_mode == InsertMode.OVERWRITE.value:
        add_data_to_postgres_overwrite(local_df, constants.INTERNET_SPEED)
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
