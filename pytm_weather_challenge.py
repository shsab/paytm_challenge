import gc
import os
import pathlib
import logging
import json

import requests
from datetime import datetime
# from risk_analysis.data.safe_graph_parsing import download_dir
from functools import partial

import pyspark.sql.functions as sf
from pyspark.sql.types import *
from pyspark.sql.window import Window

from utils.general.file_helpers import filter_filepaths
from utils.general.logging_helpers import configure_logger
from utils.general.visualization import time_series_plot, bar_plot_horizontal, bar_plot
from utils.spark.session import set_spark
from utils.spark.time_series import udf_date_range
from utils.spark.dataframe import *
from utils.general.env_helper import set_env
import matplotlib.pyplot as plt
from user_agents import parse
from utils.general.constants import *

logger = logging.getLogger(__name__)


def read_weather_data():
    schema = StructType([
        StructField('STN---', IntegerType(), False),
        StructField('WBAN', IntegerType(), True),
        StructField('YEARMODA', IntegerType(), True),
        StructField('TEMP', DoubleType(), True),
        StructField('DEWP', DoubleType(), True),
        StructField('SLP', DoubleType(), True),
        StructField('STP', DoubleType(), True),
        StructField('VISIB', DoubleType(), True),
        StructField('WDSP', DoubleType(), True),
        StructField('MXSPD', DoubleType(), True),
        StructField('GUST', DoubleType(), True),
        StructField('MAX', StringType(), True),
        StructField('MIN', StringType(), True),
        StructField('PRCP', StringType(), True),
        StructField('SNDP', DoubleType(), True),
        StructField('FRSHTT', StringType(), True)
                         ]
                        )
    files = filter_filepaths(os.path.join(data_folder_path, 'raw', '2019'),
                             file_pattern='csv.gz',
                             verbose=False,
                             logger=logger)

    weather_df = read_csv(spark=spark,
                          file_path=list(files),
                          infer_schema=False,
                          schema=schema)
    weather_df = (weather_df.withColumn('MAX', sf.regexp_extract(weather_df['MAX'], '(\-*\d+.\d+)', 0).cast('double'))
                  .withColumn('MIN', sf.regexp_extract(weather_df['MIN'], '(\-*\d+.\d+)', 0).cast('double'))
                  .withColumn('PRCP', sf.regexp_extract(weather_df['PRCP'], '(\-*\d+.\d+)', 0).cast('double'))
                  .withColumn('YEARMODA', weather_df['YEARMODA'].cast('string'))
                  )
    return weather_df


def read_country_data():
    country_df = spark.read.option('header', 'true').text(os.path.join(data_folder_path, 'raw', 'countrylist.csv'))
    country_df = (country_df.where(country_df['value'].startswith('COUNTRY_ABBR') == False)
                  .withColumn('COUNTRY_ABBR', sf.substring(country_df['value'], 1, 2))
                  .withColumn('COUNTRY_FULL', sf.substring(country_df['value'], 4, 100))
                  .drop('value')
                  )
    return country_df


def read_station_data():
    station_df = read_csv(spark=spark,
                          file_path=os.path.join(data_folder_path, 'raw', 'stationlist.csv'),
                          encoding='UTF-8')
    return station_df


def transform_data():
    clean_weather_data = spark.sql('''
            select COUNTRY_ABBR country_abbr,
                   STN_NO stn_no,
                   COUNTRY_FULL country_name,
                   WBAN wban,
                   to_date(YEARMODA, 'yMMdd') date,
                   case
                    when TEMP == 9999.9 then NULL 
                    else TEMP
                   end mean_temp_f,
                   case
                    when DEWP == 9999.9 then NULL 
                    else DEWP
                   end mean_dew_point_f,
                   case
                    when SLP == 9999.9 then NULL 
                    else SLP
                   end mean_sea_level_pressure,
                   case
                    when STP == 9999.9 then NULL 
                    else STP
                   end stp,
                   case
                    when VISIB == 999.9 then NULL 
                    else VISIB
                   end visib,
                   case
                    when WDSP == 999.9 then NULL 
                    else WDSP
                   end wdsp,
                   case
                    when MXSPD == 999.9 then NULL 
                    else MXSPD
                   end mxspd,
                   case
                    when GUST == 999.9 then NULL 
                    else GUST
                   end gust,
                   case
                    when MAX == 9999.9 then NULL 
                    else MAX
                   end max_temp_in_day_f,
                   case
                    when MIN == 9999.9 then NULL 
                    else MIN
                   end min_temp_in_day_f,
                   case
                    when PRCP == 99.9 then NULL 
                    else PRCP
                   end total_precipitation_inches,
                   case
                    when SNDP == 999.9 then NULL 
                    else SNDP
                   end snow_depth_inches,
                   FRSHTT
            from weather_data 
        ''')
    frshtt_pattern = '(\d{1})(\d{1})(\d{1})(\d{1})(\d{1})(\d{1})'
    clean_weather_data = (
        clean_weather_data.withColumn('fog', sf.regexp_extract(sf.col('FRSHTT'), frshtt_pattern, 1).cast('integer'))
            .withColumn('rain', sf.regexp_extract(sf.col('FRSHTT'), frshtt_pattern, 2).cast('integer'))
            .withColumn('snow', sf.regexp_extract(sf.col('FRSHTT'), frshtt_pattern, 3).cast('integer'))
            .withColumn('hail', sf.regexp_extract(sf.col('FRSHTT'), frshtt_pattern, 4).cast('integer'))
            .withColumn('thunder', sf.regexp_extract(sf.col('FRSHTT'), frshtt_pattern, 5).cast('integer'))
            .withColumn('tornado', sf.regexp_extract(sf.col('FRSHTT'), frshtt_pattern, 6).cast('integer'))
    )
    return clean_weather_data


def check_tornado():
    w1 = Window.partitionBy('country_name').orderBy('date')
    tornado_df = spark.sql('select date, country_name, max(tornado) tornado from clean_weather_data group by date, country_name')
    tornado_df = tornado_df.withColumn('lag', sf.lag('tornado').over(w1))
    tornado_df = tornado_df.withColumn('flag', sf.when((tornado_df['tornado'] == tornado_df['lag']) & (tornado_df['tornado'] == 1), 1).otherwise(0))
    return tornado_df.groupBy('country_name').agg(sf.sum('flag').alias('num_cons_days')).orderBy('num_cons_days', ascending=False).first()


def main():
    weather_df = read_weather_data()
    country_df = read_country_data()
    station_df = read_station_data()

    # Join the data
    station_df = station_df.join(sf.broadcast(country_df), ['COUNTRY_ABBR'])
    weather_df = weather_df.join(sf.broadcast(station_df), weather_df['STN---'] == station_df['STN_NO'])
    weather_df.persist()

    # define
    weather_df.createOrReplaceTempView('weather_data')
    clean_weather_data = transform_data()
    clean_weather_data.createOrReplaceTempView('clean_weather_data')

    hotest_avg_mean_temp = spark.sql('select country_name, avg(coalesce(mean_temp_f,0)) avg_mean_temp_f  from clean_weather_data group by country_name order by avg_mean_temp_f desc').first()
    logger.info(f"Which country had the hottest average mean temperature over the year? {hotest_avg_mean_temp.country_name}")
    highest_avg_mean_wind_speed = spark.sql('select country_name, avg(coalesce(wdsp,0)) avg_wdsp  from clean_weather_data group by country_name order by avg_wdsp desc').first()
    logger.info(f"Which country had the second highest average mean wind speed over the year? {highest_avg_mean_wind_speed.country_name}")

    # Getting the consecutive rows with the same value
    most_cons_days_tornado = check_tornado()
    logger.info(f"Which country had the most consecutive days of tornadoes/funnel cloud formations? {most_cons_days_tornado.country_name}")


if __name__ == '__main__':
    # Initializing...
    start = datetime.now()

    _env = set_env()

    # configure the logger
    if _env.get('CLUSTER_ID', None) is not None:
        logger.info(f"Cluster ID: {_env['CLUSTER_ID']}")
    if _env.get('STEP_ID', None) is not None:
        logger.info(f"Step ID: {_env['STEP_ID']}")

    configure_logger(logger=logger,
                     log_file_name='paytm_labs',
                     env_vars=_env)

    spark = set_spark(app_name="Paytm Labs Challenges",
                      use_emr=False,
                      set_s3=False,
                      spark_master=_env.get('SPARK_MASTER', 'local[*]'))

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    logger.info(f"Default parallelism: {sc.defaultParallelism}")

    # Enable garbage collection
    gc.enable()

    # setup local data and temp directories
    current_files_path = pathlib.Path(__file__).parent.absolute()
    data_folder_path = os.path.abspath(
        os.path.join(current_files_path, './data'))
    pathlib.Path(data_folder_path).mkdir(parents=True, exist_ok=True)
    tmp_folder_path = os.path.abspath(
        os.path.join(current_files_path, './data/temp'))
    pathlib.Path(tmp_folder_path).mkdir(parents=True, exist_ok=True)

    try:
        main()
    except Exception as err:
        logger.error(err)
    finally:
        logger.info(f"Total duration: {datetime.now() - start}")
        spark.stop()
