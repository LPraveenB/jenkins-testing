import argparse
import time

import transform as ts
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

SKU_COLUMN_NAME = "SKU"
LOCATION_COLUMN_NAME = "LOCATION"
DATE_COLUMN_NAME = "DATE"
DATA_SPLIT_COLUMN_NAME = "data_split"
LOCATION_GROUP_FOLDER_PREFIX = "LOCATION_GROUP="

target_columns_list = None


def get_spark_session(run_mode, max_records_per_batch):
    spark = SparkSession.builder.appName("data_split_util").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
    if run_mode == "I":
        spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", max_records_per_batch)
    return spark


def add_data_split_column_historical(input_df):
    return ts.add_split_column(input_df, train_test_frac=0.9, train_validation_frac=0.9, date_col_name=DATE_COLUMN_NAME,
                               min_df_len_to_process=270, target=target_columns_list)


def add_data_split_column_incremental(iterator):
    for df in iterator:
        result = ts.add_split_column(df, train_test_frac=0.9, train_validation_frac=0.9, date_col_name=DATE_COLUMN_NAME,
                                     min_df_len_to_process=10, target=target_columns_list)
        yield result


def prepare_schema(df_to_split):
    df_schema = StructType.fromJson(df_to_split.schema.jsonValue())
    df_schema.add(DATA_SPLIT_COLUMN_NAME, StringType())
    return df_schema


def generate_results(df, run_mode):
    if run_mode == "I":
        result = df.mapInPandas(eval("add_data_split_column_incremental"), schema=prepare_schema(df))
    else:
        result = df.groupBy(SKU_COLUMN_NAME, LOCATION_COLUMN_NAME).applyInPandas(
            eval("add_data_split_column_historical"),
            schema=prepare_schema(df))
    return result


def execute(input_path, data_split_out_path, location_group_list, max_records_per_batch, run_mode):
    for location_group in location_group_list:
        input_path = input_path + "/" + LOCATION_GROUP_FOLDER_PREFIX + location_group
        spark = get_spark_session(run_mode, max_records_per_batch)
        input_df = spark.read.parquet(input_path)

        result = generate_results(input_df, run_mode)
        out_path = data_split_out_path + "/" + LOCATION_GROUP_FOLDER_PREFIX + location_group
        (result.write.partitionBy(DATA_SPLIT_COLUMN_NAME).option("compression", "gzip").mode("overwrite").
         parquet(out_path))
        print("Successfully processed location group ", location_group)


def main(args=None):
    start_time = time.time()
    parser = argparse.ArgumentParser(description="Running data_split")
    parser.add_argument(
        '--input_path',
        dest='input_path',
        type=str,
        required=True,
        help='Base path of input dataframe to split')
    parser.add_argument(
        '--data_split_out_path',
        dest='data_split_out_path',
        type=str,
        required=True,
        help='data_split_out_path')
    parser.add_argument(
        '--location_group_list',
        dest='location_group_list',
        type=str,
        required=True,
        help='Comma separated location groups to split')
    parser.add_argument(
        '--target_column_list',
        dest='target_column_list',
        type=str,
        required=True,
        help='Comma separated target columns')
    parser.add_argument(
        '--run_mode',
        dest='run_mode',
        type=str,
        choices={'H', 'I'},
        required=True,
        help='"H" for historical mode and "I" for incremental mode')
    parser.add_argument(
        '--max_records_per_batch',
        dest='max_records_per_batch',
        type=str,
        required=False,
        default="10000",
        help='max_records_per_batch')

    args = parser.parse_args(args)
    print("args:")
    print(args)

    global target_columns_list
    target_columns_list = args.target_column_list.split(",")

    execute(args.input_path, args.data_split_out_path, args.location_group_list.split(","), args.max_records_per_batch,
            args.run_mode)
    print("<-----------Data Split Component Successful----------->")
    print('Total Time Taken', time.time() - start_time, 'Seconds')


if __name__ == '__main__':
    main()
