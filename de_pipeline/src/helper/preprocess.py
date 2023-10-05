import argparse
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import split
import sys_
import os
from resources import config
from utils import *

# Set spark environments
os.environ['PYSPARK_PYTHON'] = '/opt/conda/default/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

print('Preprocessing Job Started..')


def get_spark_session():
    """
    generate the spark session object
    """
    app_name = "Preprocessing_Job"
    spark_object = SparkSession.builder.appName(app_name).getOrCreate()
    return spark_object


spark = get_spark_session()


def preprocessing(spark, input_path, output_path, master_output_path, table_to_process):
    """
    processing the output of validator data , create location groups in it and save to output folder
    """
    print(f"folder list to process {folders_to_process}")
    folder = input_path + "/"
    if table_to_process in config.master_tables:
        spark.read.options(header='True', inferSchema='True') \
            .csv(folder + '*.csv').write.option("header", True).option("compression", "gzip").mode("overwrite") \
            .parquet(master_output_path + "/" + table_to_process + '.parquet')
    else:
        print(f"current folder {folder}")
        print(f"output_table {output_table}")
        df = spark.read.options(header='True', inferSchema='True') \
            .csv(folder + '*.csv')
        df = df.withColumn('LOCATION_GROUP', f.round((f.col('LOCATION') / 50), 0))
        df.write.option("header", True).option("compression", "gzip").mode("overwrite") \
            .partitionBy("LOCATION_GROUP").parquet(output_path + "/" + table_to_process + '.parquet')


def main(args=None):
    print("args", args)
    parser = argparse.ArgumentParser(description='Denormalizer Process')

    parser.add_argument(
        '--input_path',
        dest='input_path',
        type=str,
        required=True,
        help='Path with data to be processed for that particular day (this are the succuss files from data-validator)')

    parser.add_argument(
        '--output_path',
        dest='output_path',
        type=str,
        required=True,
        help='Path where data will be saved after preprocessing.')

    parser.add_argument(
        '--master_output_path',
        dest='master_output_path',
        type=str,
        required=True,
        help='Path where data will be saved after preprocessing.')

    parser.add_argument(
        '--dates',
        dest='dates',
        type=str,
        required=True,
        help='dates to be processed')

    parser.add_argument(
        '--table_names',
        dest='table_names',
        type=str,
        required=True,
        help='Tables to be processed')

    args = parser.parse_args(args)

    print('args.input_path', args.input_path)
    print('args.output_path', args.output_path)
    print('args.master_output_path', args.master_output_path)
    print('args.dates', args.dates)
    spark = get_spark_session()
    preprocessing(spark, args.input_path, args.output_path, args.master_output_path, table_name)


if __name__ == '__main__':
    main()
