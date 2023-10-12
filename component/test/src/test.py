import argparse
import time
import dask
import os
import numpy as np
import xgboost as xgb

from dask.distributed import Client
from dask import dataframe as dd
from distributed import LocalCluster
from google.cloud import storage
from urllib.parse import urlparse


"""
This component generates the predictions using the 4 release-01 models
Also, final OOS predictions are generated combining the above predictions

It makes use of 4 models for the same in the following manner:
Model 1 -> Binary classifier -> Predicts OOS
Model 2 -> Binary classifier -> Predicts whether inventory lower than the book
Model 3 -> Regression -> Predicts the inventory range for items whose inventory is lesser than book
Model 4 -> Regression -> Predicts inventory range for items whose inventory is greater than book

The output is generated in parquet format at the output location
It can accept a list of location groups as input and processes each location group sequentially
It can run on a local dask cluster as well as a remote dask cluster
"""


LOCATION_GROUP_FOLDER_PREFIX = "LOCATION_GROUP="
DATA_SPLIT_COLUMN_NAME = "data_split"
DATA_SPLIT_TYPE_TEST_FOLDER_PREFIX = DATA_SPLIT_COLUMN_NAME + "=test"
PARQUET_FILE_EXTENSION = ".parquet"
BINARY_PRED = "binary_pred"
ORIG_PRED = "orig_pred"
ORIG_PRED_TEMP = "orig_pred_temp"
ORIG_PRED_LOW = "orig_pred_low"
ORIG_PRED_HIGH = "orig_pred_high"
HL_ORIG_PRED = "hl_orig_pred"
CURDAY_IP_QTY_EOP_SOH = "CURDAY_IP_QTY_EOP_SOH"
OOS = "OOS"
STEP_1 = "step_1"
STEP_2 = "step_2"
STEP_3 = "step_3"
STEP_4 = "step_4"
FINAL_PREDICTION = "final_prediction"


def get_local_dask_cluster(num_workers_local_cluster, num_threads_per_worker, memory_limit_local_worker):
    dask.config.set({"dataframe.shuffle.method": "tasks"})
    dask_cluster = LocalCluster(n_workers=num_workers_local_cluster, threads_per_worker=num_threads_per_worker,
                                memory_limit=memory_limit_local_worker)
    dask_client = Client(dask_cluster)
    dask.config.set({"dataframe.shuffle.method": "tasks"})
    return dask_client


def get_remote_dask_client(dask_address, dask_connection_timeout):
    dask.config.set({"dataframe.shuffle.method": "tasks"})
    dask_client = Client(dask_address, timeout=dask_connection_timeout)
    dask.config.set({"dataframe.shuffle.method": "tasks"})
    return dask_client


def download_model(client, prod_model_dir, local_dir, local_file_name):
    gcs_path = urlparse(prod_model_dir, allow_fragments=False)
    bucket_name = gcs_path.netloc
    path = gcs_path.path.lstrip('/')
    bucket = client.bucket(bucket_name=bucket_name)
    blob = bucket.blob(path)
    local_full_path = os.path.join(local_dir, local_file_name)
    blob.download_to_filename(local_full_path)


def download_and_load_model(gcs_client, step_name, source_path_model):
    print(f'Loading model: {step_name}')
    model_local_path = ".."
    model_local_filename = f'model_{step_name}.json'
    download_model(gcs_client, source_path_model, model_local_path, model_local_filename)
    loaded_model = None
    if step_name in [STEP_1, STEP_2]:
        loaded_model = xgb.dask.DaskXGBClassifier()
    elif step_name in [STEP_3, STEP_4]:
        loaded_model = xgb.dask.DaskXGBRegressor()
    loaded_model.load_model(model_local_filename)
    feature_names = loaded_model.get_booster().feature_names
    return loaded_model, feature_names


def read_input_data(dd_df, input_path, dask_client):
    print('Reading from', input_path)
    input_data_dd = dd_df.read_parquet(input_path, engine='pyarrow', calculate_divisions=False)
    input_data_dd = dask_client.persist(input_data_dd)
    return input_data_dd


def reverse_calc_daily_reg(df):
    df[ORIG_PRED_TEMP] = round(round(df[ORIG_PRED], 2) * df[CURDAY_IP_QTY_EOP_SOH])
    df[ORIG_PRED_LOW] = round(df[ORIG_PRED_TEMP] - (df[ORIG_PRED_TEMP] * 0.15))
    df[ORIG_PRED_HIGH] = round(df[ORIG_PRED_TEMP] + (df[ORIG_PRED_TEMP] * 0.15))
    df[ORIG_PRED_LOW] = df[ORIG_PRED_LOW].apply(np.floor)
    df[ORIG_PRED_HIGH] = df[ORIG_PRED_HIGH].apply(np.ceil)

    df.loc[(df[ORIG_PRED_LOW] == df[ORIG_PRED_HIGH]), ORIG_PRED_LOW] \
        = df[ORIG_PRED_LOW] - 2
    df.loc[(df[ORIG_PRED_HIGH] - df[ORIG_PRED_LOW]) == 1, ORIG_PRED_LOW] \
        = df[ORIG_PRED_LOW] - 1
    df.loc[df[ORIG_PRED_LOW] < 0, ORIG_PRED_HIGH] = df[ORIG_PRED_HIGH] + 1
    df.loc[df[ORIG_PRED_LOW] < 0, ORIG_PRED_LOW] = 0
    return df


def prepare_for_inference(input_data_dd, feature_names):
    """
    This method checks the data type each feature column in the input dataframe
    and if it finds it to be categorical, that column is cast as an int column
    :param input_data_dd: input data frame
    :param feature_names: feature name list of the model
    :return: input data frame with no categorical feature columns
    """
    for feature in feature_names:
        if input_data_dd[feature].dtype == 'O':
            input_data_dd[feature] = input_data_dd[feature].astype(int)
    return input_data_dd


def run_predict(input_data_dd, dask_client, loaded_model_s1, feature_names_s1, loaded_model_s2, feature_names_s2,
                loaded_model_s3, feature_names_s3, loaded_model_s4, feature_names_s4, decision_threshold_step_1,
                decision_threshold_step_2):
    print('Using Binary OOS classifier: step 1')
    input_data_dd = prepare_for_inference(input_data_dd, feature_names_s1)
    input_data_dd[BINARY_PRED] = xgb.dask.predict(dask_client, loaded_model_s1.get_booster(),
                                                  input_data_dd[feature_names_s1])
    input_data_dd_all_oos = input_data_dd[(input_data_dd[BINARY_PRED] > decision_threshold_step_1)]
    step_1_dd = dask_client.persist(input_data_dd_all_oos)
    input_data_dd_all_oos[ORIG_PRED_LOW] = 0
    input_data_dd_all_oos[ORIG_PRED_HIGH] = 0
    input_data_dd_all_oos[ORIG_PRED] = 0

    print('Using Binary HL classifier: step 2')
    input_data_dd_all_rest = input_data_dd[(input_data_dd[BINARY_PRED] <= decision_threshold_step_1)]
    input_data_dd_all_rest = prepare_for_inference(input_data_dd_all_rest, feature_names_s2)
    input_data_dd_all_rest[HL_ORIG_PRED] = xgb.dask.predict(dask_client, loaded_model_s2.get_booster(),
                                                            input_data_dd_all_rest[feature_names_s2])

    print('Using LS Regression Model: step 3')
    input_data_dd_all_rest_ls = input_data_dd_all_rest[
        input_data_dd_all_rest[HL_ORIG_PRED] > decision_threshold_step_2]
    input_data_dd_all_rest_ls = prepare_for_inference(input_data_dd_all_rest_ls, feature_names_s3)
    input_data_dd_all_rest_ls[ORIG_PRED] = xgb.dask.predict(dask_client, loaded_model_s3.get_booster(),
                                                            input_data_dd_all_rest_ls[feature_names_s3])
    input_data_dd_all_rest_ls = input_data_dd_all_rest_ls.reset_index(drop=True)
    input_data_dd_all_rest_ls = input_data_dd_all_rest_ls.map_partitions(reverse_calc_daily_reg)

    print('Using HS Regression model: step 4')
    input_data_dd_all_rest_hs = input_data_dd_all_rest[
        input_data_dd_all_rest[HL_ORIG_PRED] <= decision_threshold_step_2]
    input_data_dd_all_rest_hs = prepare_for_inference(input_data_dd_all_rest_hs, feature_names_s4)
    input_data_dd_all_rest_hs[ORIG_PRED] = xgb.dask.predict(
        dask_client, loaded_model_s4.get_booster(), input_data_dd_all_rest_hs[feature_names_s4])
    input_data_dd_all_rest_hs = input_data_dd_all_rest_hs.reset_index(drop=True)
    input_data_dd_all_rest_hs = input_data_dd_all_rest_hs.map_partitions(reverse_calc_daily_reg)

    print("Concatenate results")
    input_data_dd_all_day_rev = dd.concat([input_data_dd_all_oos, input_data_dd_all_rest_ls, input_data_dd_all_rest_hs])
    # Prediction post-processing
    input_data_dd_all_day_rev[OOS] = 1
    input_data_dd_all_day_rev[OOS] = input_data_dd_all_day_rev[OOS].where(
        (input_data_dd_all_day_rev[ORIG_PRED_LOW] == 0), 0)

    return (input_data_dd_all_day_rev, step_1_dd, input_data_dd_all_rest, input_data_dd_all_rest_ls,
            input_data_dd_all_rest_hs)


def execute(source_path_model_s1: str, source_path_model_s2: str, source_path_model_s3: str, source_path_model_s4: str,
            decision_threshold_step_1: float, decision_threshold_step_2: float, input_base_path: str, output_path: str,
            location_group_list: list, local_dask_flag: str, dask_address: str, dask_connection_timeout: int,
            num_workers_local_cluster: int, num_threads_per_worker: int, memory_limit_local_worker: str):
    if local_dask_flag == 'Y':
        dask_client = get_local_dask_cluster(num_workers_local_cluster, num_threads_per_worker,
                                             memory_limit_local_worker)
    else:
        dask_client = get_remote_dask_client(dask_address, dask_connection_timeout)

    gcs_client = storage.Client()
    loaded_model_s1, feature_names_s1 = download_and_load_model(gcs_client, STEP_1, source_path_model_s1)
    loaded_model_s2, feature_names_s2 = download_and_load_model(gcs_client, STEP_2, source_path_model_s2)
    loaded_model_s3, feature_names_s3 = download_and_load_model(gcs_client, STEP_3, source_path_model_s3)
    loaded_model_s4, feature_names_s4 = download_and_load_model(gcs_client, STEP_4, source_path_model_s4)

    for location_group in location_group_list:
        print("Processing LOCATION_GROUP=" + location_group)
        input_path = (input_base_path + '/' + LOCATION_GROUP_FOLDER_PREFIX + location_group +
                      '/' + DATA_SPLIT_TYPE_TEST_FOLDER_PREFIX + '/*' + PARQUET_FILE_EXTENSION)
        input_dd = read_input_data(dd, input_path, dask_client)
        print(input_dd.info())
        final_prediction_dd, step_1_dd, step_2_dd, step_3_dd, step_4_dd = run_predict(input_dd, dask_client,
                                                                                      loaded_model_s1, feature_names_s1,
                                                                                      loaded_model_s2, feature_names_s2,
                                                                                      loaded_model_s3, feature_names_s3,
                                                                                      loaded_model_s4, feature_names_s4,
                                                                                      decision_threshold_step_1,
                                                                                      decision_threshold_step_2)
        output_path_step_1 = output_path + "/" + LOCATION_GROUP_FOLDER_PREFIX + location_group + "/" + STEP_1
        output_path_step_2 = output_path + "/" + LOCATION_GROUP_FOLDER_PREFIX + location_group + "/" + STEP_2
        output_path_step_3 = output_path + "/" + LOCATION_GROUP_FOLDER_PREFIX + location_group + "/" + STEP_3
        output_path_step_4 = output_path + "/" + LOCATION_GROUP_FOLDER_PREFIX + location_group + "/" + STEP_4
        output_path_final_prediction = (output_path + "/" + LOCATION_GROUP_FOLDER_PREFIX + location_group + "/" +
                                        FINAL_PREDICTION)
        step_1_dd.to_parquet(output_path_step_1, schema="infer")
        step_2_dd.to_parquet(output_path_step_2, schema="infer")
        step_3_dd.to_parquet(output_path_step_3, schema="infer")
        step_4_dd.to_parquet(output_path_step_4, schema="infer")
        final_prediction_dd.to_parquet(output_path_final_prediction, schema="infer")

        print("Finished processing LOCATION_GROUP=" + location_group)


def main(args=None):
    start_time = time.time()
    parser = argparse.ArgumentParser(description="Running Test Component")
    parser.add_argument(
        '--source_path_model_s1',
        dest='source_path_model_s1',
        type=str,
        required=True,
        help='Step1 model directory')
    parser.add_argument(
        '--source_path_model_s2',
        dest='source_path_model_s2',
        type=str,
        required=True,
        help='Step2 model directory')
    parser.add_argument(
        '--source_path_model_s3',
        dest='source_path_model_s3',
        type=str,
        required=True,
        help='Step3 model directory')
    parser.add_argument(
        '--source_path_model_s4',
        dest='source_path_model_s4',
        type=str,
        required=True,
        help='Step4 model directory')
    parser.add_argument(
        '--decision_threshold_step_1',
        dest='decision_threshold_step_1',
        type=float,
        required=False,
        default=0.5,
        help='Decision threshold for Binary OOS prediction')
    parser.add_argument(
        '--decision_threshold_step_2',
        dest='decision_threshold_step_2',
        type=float,
        required=False,
        default=0.5,
        help='Decision threshold for Binary H/L prediction')
    parser.add_argument(
        '--input_base_path',
        dest='input_base_path',
        type=str,
        required=True,
        help='Input base path containing all the location groups')
    parser.add_argument(
        '--output_path',
        dest='output_path',
        type=str,
        required=True,
        help='Directory to write the predictions')
    parser.add_argument(
        '--location_group_list',
        dest='location_group_list',
        type=str,
        nargs='+',
        required=True,
        help='Space separated location groups to split')
    parser.add_argument(
        '--local_dask_flag',
        dest='local_dask_flag',
        type=str,
        choices={'Y', 'N'},
        required=True,
        help='Flag to determine whether dask is local or not')
    parser.add_argument(
        '--dask_address',
        dest='dask_address',
        type=str,
        default=None,
        required=False,
        help='Address of the remote dask cluster')
    parser.add_argument(
        '--dask_connection_timeout',
        dest='dask_connection_timeout',
        type=int,
        default=-1,
        required=False,
        help='Remote dask connection timeout in seconds')
    parser.add_argument(
        '--num_workers_local_cluster',
        dest='num_workers_local_cluster',
        type=int,
        default=0,
        required=False,
        help='Number of workers for the local dask cluster')
    parser.add_argument(
        '--num_threads_per_worker',
        dest='num_threads_per_worker',
        type=int,
        default=0,
        required=False,
        help='Number of threads per local dask cluster worker')
    parser.add_argument(
        '--memory_limit_local_worker',
        dest='memory_limit_local_worker',
        type=str,
        default=None,
        required=False,
        help='Memory limit per worker in the local dask cluster')

    args = parser.parse_args(args)
    print("args:")
    print(args)

    if args.local_dask_flag == 'Y':
        if (args.num_workers_local_cluster == 0) or (args.num_threads_per_worker == 0) or \
                (args.memory_limit_local_worker is None):
            raise ValueError("num_workers_local_cluster, num_threads_per_worker & memory_limit_local_worker need to "
                             "have valid values for a local dask cluster")
    else:
        if (args.dask_address is None) or (args.dask_connection_timeout == -1):
            raise ValueError("dask_address & dask_connection_timeout need to have valid values for remote dask cluster")

    execute(args.source_path_model_s1, args.source_path_model_s2, args.source_path_model_s3, args.source_path_model_s4,
            args.decision_threshold_step_1, args.decision_threshold_step_2, args.input_base_path, args.output_path,
            args.location_group_list, args.local_dask_flag, args.dask_address, args.dask_connection_timeout,
            args.num_workers_local_cluster, args.num_threads_per_worker, args.memory_limit_local_worker)
    print("<-----------Test Component Successful----------->")
    print('Total Time Taken', time.time() - start_time, 'Seconds')


if __name__ == '__main__':
    main()
