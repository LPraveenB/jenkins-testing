import argparse
import os
import time
import dask
import pprint
import json

from urllib.parse import urlparse
from dask.distributed import Client
from distributed import LocalCluster
from dask import dataframe as dd
from google.cloud import storage
from sklearn.metrics import accuracy_score, roc_auc_score, precision_score, recall_score, f1_score
from sklearn.metrics import r2_score, mean_squared_error

STEP_1 = "step_1"
STEP_2 = "step_2"
STEP_3 = "step_3"
STEP_4 = "step_4"
FINAL_PREDICTION = "final_prediction"

CLASSIFICATION_BINARY = "classification_binary"
REGRESSION = "regression"

TARGET_CLASSIFICATION_BINARY = "BINARY_TARGET_CLASSIFICATION"
TARGET_CLASSIFICATION_BINARY_HL = "BINARY_HL_TARGET_CLASSIFICATION"
TARGET_REGRESSION = "TARGET_REGRESSION"

BINARY_PRED = "binary_pred"
ORIG_PRED = "orig_pred"
ORIG_PRED_TEMP = "orig_pred_temp"
ORIG_PRED_LOW = "orig_pred_low"
ORIG_PRED_HIGH = "orig_pred_high"
HL_ORIG_PRED = "hl_orig_pred"
IP_QTY_EOP_SOH = "IP_QTY_EOP_SOH"
REV_IP_QTY_EOP_SOH = "rev_ip_qty_eop_soh"
CURDAY_IP_QTY_EOP_SOH = "CURDAY_IP_QTY_EOP_SOH"
TOTAL_RETAIL = "TOTAL_RETAIL"


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


def read_dataframe(dd_df, input_path, dask_client):
    print('Reading from', input_path)
    input_data_dd = dd_df.read_parquet(input_path, engine='pyarrow', calculate_divisions=False)
    input_data_dd = dask_client.persist(input_data_dd)
    return input_data_dd


def upload_metrics_to_gcs(local_metrics_filename, metrics, metrics_output_file_path):
    print(f'Writing {local_metrics_filename} to local')
    with open(local_metrics_filename, 'w') as f:
        json.dump(metrics, f)
    local_dir = '.'
    local_full_path = os.path.join(local_dir, local_metrics_filename)

    gcs_client = storage.Client()
    gcs_path = urlparse(metrics_output_file_path)
    bucket_name = gcs_path.netloc
    gcs_upload_dir = gcs_path.path.lstrip('/')
    bucket = gcs_client.bucket(bucket_name=bucket_name)
    blob = bucket.blob(gcs_upload_dir)
    blob.upload_from_filename(local_full_path)
    print("Successfully uploaded metrics to GCS")


def calculate_technical_metrics(step_name, scoring_type, dd_df, target, prediction_column, decision_threshold=0.5):
    round_digit = 3
    print(f'Getting metrics for: {step_name}')
    y_true = dd_df[target].compute()
    y_pred_score = dd_df[prediction_column].compute()

    result = None
    if scoring_type == CLASSIFICATION_BINARY:
        y_pred = y_pred_score.apply(lambda x: 1 if x > decision_threshold else 0)
        accuracy = round(accuracy_score(y_true, y_pred), round_digit)
        roc_auc = round(roc_auc_score(y_true, y_pred_score), round_digit) if len(y_true.unique()) > 1 else 0.0
        precision = round(precision_score(y_true, y_pred), round_digit)
        recall = round(recall_score(y_true, y_pred), round_digit)
        f1 = round(f1_score(y_true, y_pred), round_digit)
        result = {
            f'{step_name}_accuracy': accuracy,
            f'{step_name}_roc_auc': roc_auc,
            f'{step_name}_precision': precision,
            f'{step_name}_recall': recall,
            f'{step_name}_f1': f1}

    elif scoring_type == REGRESSION:
        mean_squared_err = round(mean_squared_error(y_true, y_pred_score), round_digit) if len(y_true) > 1 else 0.0
        r2 = r2_score(y_true, y_pred_score) if len(y_true) > 1 else 0.0
        n = len(y_true)
        k = dd_df.shape[1] - 1  # Number of columns minus target
        adjusted_r2 = 1 - ((1 - r2) * (n - 1) / (n - k - 1))
        adjusted_r2 = round(adjusted_r2, round_digit)
        r2 = round(r2, round_digit)
        result = {
            f'{step_name}_mean_squared_err': mean_squared_err,
            f'{step_name}_r2': r2,
            f'{step_name}_adjusted_r2': adjusted_r2}
    pprint.pprint(result)
    return result


def calculate_overall_metrics(df):
    stock_threshold = 0
    tp = len(df[(df[REV_IP_QTY_EOP_SOH] <= stock_threshold) & (df[ORIG_PRED_LOW] <= stock_threshold)])
    fn = len(df[(df[REV_IP_QTY_EOP_SOH] <= stock_threshold) & (df[ORIG_PRED_LOW] > stock_threshold)])
    tn = len(df[(df[REV_IP_QTY_EOP_SOH] > stock_threshold) & (df[ORIG_PRED_LOW] > stock_threshold)])
    fp = len(df[(df[REV_IP_QTY_EOP_SOH] > stock_threshold) & (df[ORIG_PRED_LOW] <= stock_threshold)])

    tp_perc = 0
    fn_perc = 0
    recall_perc = 0
    if (tp + fn) > 0:
        tp_perc = round((tp / (tp + fn)) * 100, 2)
        fn_perc = round((fn / (tp + fn)) * 100, 2)
        recall_perc = round((tp / (tp + fn)) * 100, 2)
    print('TP %', tp_perc)
    print('FN %', fn_perc)
    print('Recall %', recall_perc)

    tn_perc = 0
    fp_perc = 0
    if (tn + fp) > 0:
        tn_perc = round((tn / (tn + fp)) * 100, 2)
        fp_perc = round((fp / (tn + fp)) * 100, 2)
    print('TN %', tn_perc)
    print('FP %', fp_perc)

    prec_perc = 0
    if (tp + fp) > 0:
        prec_perc = round((tp / (tp + fp)) * 100, 2)
    print('Precision %', prec_perc)

    f1_score = 0
    if (prec_perc + recall_perc) > 0:
        f1_score = round(2 * ((prec_perc * recall_perc) / (prec_perc + recall_perc)), 2)
    print("F1 Score: ", f1_score)

    accuracy_perc = 0
    if (tp + tn + fp + fn) > 0:
        accuracy_perc = round(((tp + tn) / (tp + tn + fp + fn)) * 100, 2)
    print("Accuracy%: ", accuracy_perc)

    df_low = df[df[REV_IP_QTY_EOP_SOH] <= df[CURDAY_IP_QTY_EOP_SOH]]
    df_low_len = len(df_low)
    print("df_low_len: ", df_low_len)

    under_stock_acc = len(df_low[(df_low[ORIG_PRED_LOW] <= df_low[REV_IP_QTY_EOP_SOH]) &
                                 (df_low[ORIG_PRED_HIGH] >= df_low[REV_IP_QTY_EOP_SOH])])
    under_stock_acc_perf = round((under_stock_acc / df_low_len) * 100, 2)
    print("Actual Less Than Book Accurate Performance %", under_stock_acc_perf)

    under_stock_overall = len(df_low[(df_low[ORIG_PRED_LOW] <= df_low[CURDAY_IP_QTY_EOP_SOH])])
    under_stock_overall_perf = round((under_stock_overall / df_low_len) * 100, 2)
    print("Actual Less Than Book Overall  Performance %", under_stock_overall_perf)

    df_high = df[df[REV_IP_QTY_EOP_SOH] > df[CURDAY_IP_QTY_EOP_SOH]]
    df_high_len = len(df_high)
    print("df_high_len: ", df_high_len)

    over_stock_acc = len(df_high[(df_high[ORIG_PRED_LOW] <= df_high[REV_IP_QTY_EOP_SOH]) &
                                 (df_high[ORIG_PRED_HIGH] >= df_high[REV_IP_QTY_EOP_SOH])])
    over_stock_acc_perf = round((over_stock_acc / df_high_len) * 100, 2)
    print('Actual Greater Than Book Accurate  Performance %', over_stock_acc_perf)

    over_stock_overall = len(df_high[(df_high[ORIG_PRED_HIGH] > df_high[CURDAY_IP_QTY_EOP_SOH])])
    over_stock_overall_perf = round((over_stock_overall / df_high_len) * 100, 2)
    print('Actual Greater Than Book Overall  Performance %', over_stock_overall_perf)

    df_len = len(df)

    br_pr = len(df[(df[ORIG_PRED_LOW] <= stock_threshold) & (df[IP_QTY_EOP_SOH] <= stock_threshold) &
                   (df[TARGET_CLASSIFICATION_BINARY] == 1)])
    br_pr_perc = round((br_pr / df_len) * 100, 2)
    print("BR_PR %: ", br_pr_perc)
    br_pr_value = df[(df[ORIG_PRED_LOW] <= stock_threshold) & (df[IP_QTY_EOP_SOH] <= stock_threshold) &
                     (df[TARGET_CLASSIFICATION_BINARY] == 1)][TOTAL_RETAIL].sum().compute()
    print("BR_PR $ Value: ", br_pr_value)

    br_pw = len(df[(df[ORIG_PRED_LOW] > stock_threshold) & (df[IP_QTY_EOP_SOH] <= stock_threshold) &
                   (df[TARGET_CLASSIFICATION_BINARY] == 1)])
    br_pw_perc = round((br_pw / df_len) * 100, 2)
    print("BR_PW %: ", br_pw_perc)

    bw_pr = len(df[(df[ORIG_PRED_LOW] <= stock_threshold) & (df[IP_QTY_EOP_SOH] > stock_threshold) &
                   (df[TARGET_CLASSIFICATION_BINARY] == 1)])
    bw_pr_perc = round((bw_pr / df_len) * 100, 2)
    print("BW_PR %: ", bw_pr_perc)

    revenue_recovery_due_to_model = df[(df[ORIG_PRED_LOW] <= stock_threshold) & (df[IP_QTY_EOP_SOH] > stock_threshold) &
                                       (df[TARGET_CLASSIFICATION_BINARY] == 1)][TOTAL_RETAIL].sum().compute()
    print("Revenue Recovery Due to Model: ", revenue_recovery_due_to_model)
    loss_due_to_model = df[(df[ORIG_PRED_LOW] > stock_threshold) &
                           (df[TARGET_CLASSIFICATION_BINARY] == 1)][TOTAL_RETAIL].sum().compute()
    print("Loss Due to Model: ", loss_due_to_model)
    book_recovery_of_loss_due_to_model = df[(df[ORIG_PRED_LOW] > stock_threshold) &
                                            (df[TARGET_CLASSIFICATION_BINARY] == 1) &
                                            (df[IP_QTY_EOP_SOH] <= stock_threshold)][TOTAL_RETAIL].sum().compute()
    print("Part of Loss Due to Model that Book Can Recover: ", book_recovery_of_loss_due_to_model)

    result = {
        "TP %": tp_perc,
        "TN %": tn_perc,
        "FP %": fp_perc,
        "FN %": fn_perc,
        "Precision %": prec_perc,
        "Recall %": recall_perc,
        "F1 Score": f1_score,
        "Accuracy %": accuracy_perc,
        "Actual Less Than Book Accurate Performance %": under_stock_acc_perf,
        "Actual Less Than Book Overall  Performance %": under_stock_overall_perf,
        "Actual Greater Than Book Accurate  Performance %": over_stock_acc_perf,
        "Actual Greater Than Book Overall  Performance %": over_stock_overall_perf,
        "BR_PR %": br_pr_perc,
        "BR_PW %": br_pw_perc,
        "BW_PR %": bw_pr_perc,
        "BR_PR $ Value": int(br_pr_value),
        "Revenue Recovery Due to Model": int(revenue_recovery_due_to_model),
        "Loss Due to Model": int(loss_due_to_model),
        "Part of Loss Due to Model that Book Can Recover": int(book_recovery_of_loss_due_to_model)
    }
    return result


def execute(new_model_predictions_base_path: str, prod_model_predictions_base_path: str, new_model_score_base_path: str,
            prod_model_score_base_path: str, decision_threshold_step_1: float, decision_threshold_step_2: float,
            local_dask_flag: str, dask_address: str, dask_connection_timeout: int, num_workers_local_cluster: int,
            num_threads_per_worker: int, memory_limit_local_worker: str):
    if local_dask_flag == 'Y':
        dask_client = get_local_dask_cluster(num_workers_local_cluster, num_threads_per_worker,
                                             memory_limit_local_worker)
    else:
        dask_client = get_remote_dask_client(dask_address, dask_connection_timeout)

    oos_predictions_dd_new = read_dataframe(dd, new_model_predictions_base_path + '/*/' + STEP_1, dask_client)
    binary_hl_predictions_dd_new = read_dataframe(dd, new_model_predictions_base_path + '/*/' + STEP_2, dask_client)
    regression_lb_predictions_dd_new = read_dataframe(dd, new_model_predictions_base_path + '/*/' + STEP_3, dask_client)
    regression_hb_predictions_dd_new = read_dataframe(dd, new_model_predictions_base_path + '/*/' + STEP_4, dask_client)
    final_predictions_dd_new = read_dataframe(dd, new_model_predictions_base_path + '/*/' + FINAL_PREDICTION,
                                              dask_client)

    oos_predictions_dd_prod = read_dataframe(dd, prod_model_predictions_base_path + '/*/' + STEP_1, dask_client)
    binary_hl_predictions_dd_prod = read_dataframe(dd, prod_model_predictions_base_path + '/*/' + STEP_2, dask_client)
    regression_lb_predictions_dd_prod = read_dataframe(dd, prod_model_predictions_base_path + '/*/' + STEP_3,
                                                       dask_client)
    regression_hb_predictions_dd_prod = read_dataframe(dd, prod_model_predictions_base_path + '/*/' + STEP_4,
                                                       dask_client)
    final_predictions_dd_prod = read_dataframe(dd, prod_model_predictions_base_path + '/*/' + FINAL_PREDICTION,
                                               dask_client)

    tech_metrics_new = {}

    oos_score_new = calculate_technical_metrics(STEP_1, CLASSIFICATION_BINARY, oos_predictions_dd_new,
                                                TARGET_CLASSIFICATION_BINARY, BINARY_PRED, decision_threshold_step_1)
    tech_metrics_new.update(oos_score_new)
    binary_hl_score_new = calculate_technical_metrics(STEP_2, CLASSIFICATION_BINARY, binary_hl_predictions_dd_new,
                                                      TARGET_CLASSIFICATION_BINARY_HL, HL_ORIG_PRED,
                                                      decision_threshold_step_2)
    tech_metrics_new.update(binary_hl_score_new)
    regression_lb_score_new = calculate_technical_metrics(STEP_3, REGRESSION, regression_lb_predictions_dd_new,
                                                          TARGET_REGRESSION, ORIG_PRED)
    tech_metrics_new.update(regression_lb_score_new)
    regression_hb_score_new = calculate_technical_metrics(STEP_4, REGRESSION, regression_hb_predictions_dd_new,
                                                          TARGET_REGRESSION, ORIG_PRED)
    tech_metrics_new.update(regression_hb_score_new)

    upload_metrics_to_gcs(local_metrics_filename="individual_tech_metrics_new.json", metrics=tech_metrics_new,
                          metrics_output_file_path=new_model_score_base_path + "/individual_tech_metrics_new.json")

    tech_metrics_prod = {}

    oos_score_prod = calculate_technical_metrics(STEP_1, CLASSIFICATION_BINARY, oos_predictions_dd_prod,
                                                 TARGET_CLASSIFICATION_BINARY, BINARY_PRED, decision_threshold_step_1)
    tech_metrics_prod.update(oos_score_prod)
    binary_hl_score_prod = calculate_technical_metrics(STEP_2, CLASSIFICATION_BINARY, binary_hl_predictions_dd_prod,
                                                       TARGET_CLASSIFICATION_BINARY_HL, HL_ORIG_PRED,
                                                       decision_threshold_step_2)
    tech_metrics_prod.update(binary_hl_score_prod)
    regression_lb_score_prod = calculate_technical_metrics(STEP_3, REGRESSION, regression_lb_predictions_dd_prod,
                                                           TARGET_REGRESSION, ORIG_PRED)
    tech_metrics_prod.update(regression_lb_score_prod)
    regression_hb_score_prod = calculate_technical_metrics(STEP_4, REGRESSION, regression_hb_predictions_dd_prod,
                                                           TARGET_REGRESSION, ORIG_PRED)
    tech_metrics_prod.update(regression_hb_score_prod)
    upload_metrics_to_gcs(local_metrics_filename="individual_tech_metrics_prod.json", metrics=tech_metrics_prod,
                          metrics_output_file_path=prod_model_score_base_path + "/individual_tech_metrics_prod.json")

    overall_metrics_new = calculate_overall_metrics(final_predictions_dd_new)
    overall_metrics_prod = calculate_overall_metrics(final_predictions_dd_prod)
    upload_metrics_to_gcs(local_metrics_filename="overall_metrics_new.json", metrics=overall_metrics_new,
                          metrics_output_file_path=prod_model_score_base_path + "/overall_metrics_new.json")
    upload_metrics_to_gcs(local_metrics_filename="overall_metrics_prod.json", metrics=overall_metrics_prod,
                          metrics_output_file_path=prod_model_score_base_path + "/overall_metrics_prod.json")


def main(args=None):
    start_time = time.time()
    parser = argparse.ArgumentParser(description="Running Scoring Component")
    parser.add_argument(
        '--new_model_predictions_base_path',
        dest='new_model_predictions_base_path',
        type=str,
        required=True,
        help='Directory to read the predictions made by the new models')
    parser.add_argument(
        '--prod_model_predictions_base_path',
        dest='prod_model_predictions_base_path',
        type=str,
        required=True,
        help='Directory to read the predictions made by the production models')
    parser.add_argument(
        '--new_model_score_base_path',
        dest='new_model_score_base_path',
        type=str,
        required=True,
        help='Base path to the new model scores')
    parser.add_argument(
        '--prod_model_score_base_path',
        dest='prod_model_score_base_path',
        type=str,
        required=True,
        help='Base path to the prod model scores')
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

    execute(args.new_model_predictions_base_path, args.prod_model_predictions_base_path, args.new_model_score_base_path,
            args.prod_model_score_base_path, args.decision_threshold_step_1, args.decision_threshold_step_2,
            args.local_dask_flag, args.dask_address, args.dask_connection_timeout, args.num_workers_local_cluster,
            args.num_threads_per_worker, args.memory_limit_local_worker)

    print("<-----------Scoring Component Successful----------->")
    print('Total Time Taken', time.time() - start_time, 'Seconds')


if __name__ == '__main__':
    main()
