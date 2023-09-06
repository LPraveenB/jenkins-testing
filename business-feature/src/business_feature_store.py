import os
from pyspark.sql import functions as F
import time
import argparse
from table_mapping_config import TableMappingConfiguration
from pyspark.sql import Window
import utils
from pyspark.sql.types import *
import functools
from datetime import datetime, timedelta

"""
spark-submit <SPARK_OPTIONS> business_feature_store.py

  --location_groups LOCATION_GROUPS
                        comma seperated list of location group IDs to be processed
  --mapping_path MAPPING_PATH
                        GCS Location of the table mapping JSON
  --denorm_src_path DENORM_SRC_PATH
                        GCS Path where the consolidated denormalized parquets are located
  --meta_src_path META_SRC_PATH
                        GCS Path where the meta column parquets are located
  --dest_path DEST_PATH
                        GCS Path where the daily incremental feature store parquet to be stored
  --historical_mode HISTORICAL_MODE
                        Y or N indicating whether to run in historical mode or Incremental mode, Y for Historical and N for Incremental
  --current_date_str CURRENT_DATE_STR
                        The string value of the current date in the format %Y-%m-%d
"""

# os.environ['PYSPARK_PYTHON'] = '/opt/conda/default/bin/python'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
print('Starting raw_interface_transform')


def fetch_past_date_string(current_date_str, past_days_count, date_format):
    current_date_obj = datetime.strptime(current_date_str, date_format)
    past_date_obj = current_date_obj - timedelta(days=past_days_count)
    return past_date_obj.strftime(date_format)


def compute_rolling(df, config_dict, sku_col_name, loc_col_name, date_col_name):
    window_spec = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name))
    window_spec_7 = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name)).rowsBetween(-6, 0)
    window_spec_14 = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name)).rowsBetween(-13, 0)
    window_spec_28 = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name)).rowsBetween(-27, 0)
    window_spec_56 = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name)).rowsBetween(-55, 0)
    window_spec_84 = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name)).rowsBetween(-83, 0)

    # start inventory_turn_over_ratio_week
    df = df.withColumn('itr_cogs_7', F.sum(config_dict['sales']['dest_dict']['sales_units']).over(window_spec_7))
    df = df.withColumn('itr_avg_start_7',
                       F.lag(config_dict['inventory_snapshot']['dest_dict']['day_end_soh_balance'], 7).over(
                           window_spec))

    df = df.withColumn('itr_cogs_14', F.sum(config_dict['sales']['dest_dict']['sales_units']).over(window_spec_14))
    df = df.withColumn('itr_avg_start_14',
                       F.lag(config_dict['inventory_snapshot']['dest_dict']['day_end_soh_balance'], 14).over(
                           window_spec))

    df = df.withColumn('itr_cogs_28', F.sum(config_dict['sales']['dest_dict']['sales_units']).over(window_spec_28))
    df = df.withColumn('itr_avg_start_28',
                       F.lag(config_dict['inventory_snapshot']['dest_dict']['day_end_soh_balance'], 28).over(
                           window_spec))
    # end

    # start sell_through_rate
    df = df.withColumn('in_28', F.sum('ONHAND_RECORDED').over(window_spec_28))
    df = df.withColumn('out_28', F.sum(config_dict['sales']['dest_dict']['sales_units']).over(window_spec_28))

    df = df.withColumn('in_56', F.sum('ONHAND_RECORDED').over(window_spec_56))
    df = df.withColumn('out_56', F.sum(config_dict['sales']['dest_dict']['sales_units']).over(window_spec_56))

    df = df.withColumn('in_84', F.sum('ONHAND_RECORDED').over(window_spec_84))
    df = df.withColumn('out_84', F.sum(config_dict['sales']['dest_dict']['sales_units']).over(window_spec_84))

    # end sell_through_rate

    # start sales  ratio

    df = df.withColumn('cur_sales_7_28', F.sum(config_dict['sales']['dest_dict']['sales_units']).over(window_spec_7))

    df = df.withColumn('hist_sales_7_28', F.sum(config_dict['sales']['dest_dict']['sales_units']).over(window_spec_28))

    df = df.withColumn('cur_sales_28_84', F.sum(config_dict['sales']['dest_dict']['sales_units']).over(window_spec_28))

    df = df.withColumn('hist_sales_28_84', F.sum(config_dict['sales']['dest_dict']['sales_units']).over(window_spec_84))

    # end  sales ratio

    # start  stock_ratio_internal

    df = df.withColumn('cur_stock_7_28',
                       F.sum(config_dict['inventory_snapshot']['dest_dict']['day_end_soh_balance']).over(window_spec_7))

    df = df.withColumn('hist_stock_7_28',
                       F.sum(config_dict['inventory_snapshot']['dest_dict']['day_end_soh_balance']).over(
                           window_spec_28))

    df = df.withColumn('cur_stock_28_84',
                       F.sum(config_dict['inventory_snapshot']['dest_dict']['day_end_soh_balance']).over(
                           window_spec_28))

    df = df.withColumn('hist_stock_28_84',
                       F.sum(config_dict['inventory_snapshot']['dest_dict']['day_end_soh_balance']).over(
                           window_spec_84))

    # nd

    # start  out_of_stock

    df = df.withColumn('oos_indicator',
                       F.when(F.col(config_dict['inventory_snapshot']['dest_dict']['day_end_soh_balance']) <= 0,
                              F.lit(1)).otherwise(F.lit(0)))
    df = df.withColumn('oos_count_28',
                       F.sum('oos_indicator').over(window_spec_28))

    df = df.withColumn('oos_count_84',
                       F.sum('oos_indicator').over(window_spec_84))

    # start sale_day_ratio

    df = df.withColumn('hist_sale_day', F.when(F.col('SALES_UNITS_WITHOUT_RETURN') > 0, F.col(date_col_name)))

    df = df.withColumn('last_sale_day', F.last('hist_sale_day', ignorenulls=True).over(window_spec))

    df = df.withColumn('first_date', F.first(date_col_name).over(window_spec))

    df = df.withColumn('last_sale_day', F.coalesce('last_sale_day', 'first_date'))

    df = df.withColumn('hist_sale_day_count_28',
                       F.count('hist_sale_day').over(window_spec_28))

    df = df.withColumn('hist_sale_day_count_84',
                       F.count('hist_sale_day').over(window_spec_84))

    # end

    # start  categorical shrinkage

    df = df.withColumn('shrinkage_qty', F.when(F.col(config_dict['derived_fields']['dest_dict']['audit_adj_units']) < 0,
                                               F.abs(F.col(config_dict['derived_fields']['dest_dict'][
                                                               'audit_adj_units']))).otherwise(F.lit(0)))
    window_spec_cat_7 = Window.partitionBy(loc_col_name, config_dict['sku']['dest_dict']['category']).orderBy(
        F.col(date_col_name)).rowsBetween(-6, 0)
    window_spec_loc = Window.partitionBy(loc_col_name, date_col_name)
    df_cat_group = df.groupBy([config_dict['sku']['dest_dict']['category'], loc_col_name, date_col_name]).agg(
        F.sum('shrinkage_qty').alias('shrinkage_qty_day_sum'))
    df_cat_group = df_cat_group.withColumn('categorical_shrinkage_qty_7',
                                           F.sum('shrinkage_qty_day_sum').over(window_spec_cat_7))
    df_cat_group = df_cat_group.withColumn('categorical_shrinkage_scale_7',
                                           (F.col('categorical_shrinkage_qty_7') - F.min(
                                               F.col('categorical_shrinkage_qty_7')).over(window_spec_loc)) / (
                                                   F.max(F.col('categorical_shrinkage_qty_7')).over(
                                                       window_spec_loc) - F.min(
                                               F.col('categorical_shrinkage_qty_7')).over(
                                               window_spec_loc)))

    df = df.join(df_cat_group, [config_dict['sku']['dest_dict']['category'], loc_col_name, date_col_name], 'left')
    df = df.na.fill(value=0.0, subset=['categorical_shrinkage_scale_7'])
    df = df.withColumn('categorical_shrinkage_scale_7', F.round(F.col('categorical_shrinkage_scale_7'), 2))
    # df = df.withColumn('categorical_shrinkage_qty_28', F.sum('shrinkage_qty').over(window_spec_cat_28))

    # end categorical shrinkage

    #  start  return_index_internal
    window_spec_sku_loc_7 = Window.partitionBy(sku_col_name, loc_col_name).orderBy(
        F.col(date_col_name)).rowsBetween(-6, 0)

    window_spec_sku_loc_28 = Window.partitionBy(sku_col_name, loc_col_name).orderBy(
        F.col(date_col_name)).rowsBetween(-27, 0)

    #    df = df.withColumn('avg_return_7', F.sum(F.abs(config_dict['returns']['dest_dict']['return_units'])).over(window_spec_sku_loc_7))
    #    df = df.withColumn('avg_return_28',
    #                       F.sum(F.abs(config_dict['returns']['dest_dict']['return_units'])).over(window_spec_sku_loc_28))

    # end

    # start  sales_index_internal
    #   df = df.withColumn('avg_sale_7',
    #                      F.sum(config_dict['sales']['dest_dict']['sales_units']).over(window_spec_sku_loc_7))
    df = df.withColumn('avg_sale_28',
                       F.sum(config_dict['sales']['dest_dict']['sales_units']).over(window_spec_sku_loc_28))

    # end

    # start price_index

    df = df.withColumn('NAN_SALES_UNIT_RETAIL',
                       F.when(F.col(config_dict['derived_fields']['dest_dict']['sales_unit_retail']) != 0,
                              F.col(config_dict['derived_fields']['dest_dict']['sales_unit_retail'])))
    df = df.withColumn('avg_price_7',
                       F.avg('NAN_SALES_UNIT_RETAIL').over(window_spec_sku_loc_7))
    #  df = df.withColumn('avg_price_28',
    #                     F.avg('NAN_SALES_UNIT_RETAIL').over(window_spec_sku_loc_28))
    # end

    df = df.withColumn('AVG_IP_QTY_EOP_SOH_7', F.avg(F.col('IP_QTY_EOP_SOH')).over(window_spec_7))
    df = df.withColumn('AVG_IP_QTY_EOP_SOH_14', F.avg(F.col('IP_QTY_EOP_SOH')).over(window_spec_14))
    df = df.withColumn('AVG_IP_QTY_EOP_SOH_28', F.avg(F.col('IP_QTY_EOP_SOH')).over(window_spec_28))
    return df


def compute_out_of_stock_ratio(df, config_dict, day_count):
    df = df.withColumn('oos_ratio_' + str(day_count),
                       F.round(F.col('oos_count_' + str(day_count)) / F.lit(day_count), 2))
    return df


def compute_events_flag(df, config_dict):
    df = df.withColumn('transfer_flag',
                       F.when(F.col(config_dict['store_transfers']['dest_dict']['in_transit_units']) > 0,
                              F.lit(1)).otherwise(F.lit(0)))

    df = df.withColumn('sales_flag',
                       F.when(F.col(config_dict['derived_fields']['dest_dict']['sales_units_without_return']) > 0,
                              F.lit(1)).otherwise(F.lit(0)))

    df = df.withColumn('returns_flag',
                       F.when(F.col(config_dict['returns']['dest_dict']['return_units']) != 0,
                              F.lit(1)).otherwise(F.lit(0)))

    df = df.withColumn('neg_cc_flag',
                       F.when(F.col(config_dict['inventory_adjustment']['dest_dict']['neg_adj_units']) != 0,
                              F.lit(1)).otherwise(F.lit(0)))

    df = df.withColumn('pos_cc_flag',
                       F.when(F.col(config_dict['inventory_adjustment']['dest_dict']['pos_adj_units']) != 0,
                              F.lit(1)).otherwise(F.lit(0)))

    df = df.withColumn('audit_day_flag',
                       F.when(F.col(config_dict['yearly_audit_schedule']['dest_dict']['audit_flag']) == 1,
                              F.lit(1)).otherwise(F.lit(0)))

    return df


def compute_index_features(df, config_dict, day_count, avg_prefix, scale_prefix, window_spec):
    avg_column = avg_prefix + str(day_count)
    df = df.na.fill(value=0.0, subset=[avg_column])
    # df = df.withColumn(scale_prefix + str(day_count) + '_min',  F.min(F.col(avg_column)).over(window_spec))
    # df = df.withColumn(scale_prefix + str(day_count) + '_max', F.max(F.col(avg_column)).over(window_spec))
    df = df.withColumn(scale_prefix + str(day_count),
                       (F.col(avg_column) - F.min(F.col(avg_column)).over(window_spec)) / (
                                   F.max(F.col(avg_column)).over(window_spec) - F.min(F.col(avg_column)).over(
                               window_spec)))
    df = df.withColumn(scale_prefix + str(day_count), F.round(F.col(scale_prefix + str(day_count)), 2))

    return df


def compute_sale_day_ratio(df, config_dict, hist_day_count):
    # df = df.sort_values(by=[config_dict['inventory_snapshot']['dest_dict']['date']])
    hist_sales_col = 'hist_sale_day_count_' + str(hist_day_count)

    df = df.withColumn('last_sale_day_diff_' + str(hist_day_count),
                       F.datediff(F.col(config_dict['inventory_snapshot']['dest_dict']['date']),
                                  F.col('last_sale_day')))

    df = df.withColumn(hist_sales_col, custom_ratio_udf(F.lit(hist_day_count), F.col(hist_sales_col)))
    '''
    df = df.withColumn(hist_sales_col,
                       F.when(F.col(hist_sales_col) != 0,
                              F.lit(hist_day_count) / F.col(hist_sales_col)).otherwise(F.lit(100)))

    '''
    df = df.withColumn('sale_day_ratio_' + str(hist_day_count),
                       F.round(F.col('last_sale_day_diff_' + str(hist_day_count)) / F.col(hist_sales_col), 2))

    return df


def custom_divide(hist, cur, ratio):
    try:
        hist_sales = hist / ratio
        if hist_sales != 0:
            return round(cur / hist_sales, 2)
        else:
            return 100.0
    except (TypeError, ZeroDivisionError):
        return 100.0



custom_divide_udf = F.udf(custom_divide, DoubleType())


def compute_stock_ratio(df, config_dict, cur_day_count, hist_day_count):
    # df = df.sort_values(by=[config_dict['inventory_snapshot']['dest_dict']['date']])
    cur_hist_ratio = hist_day_count / cur_day_count
    cur_sales_col = 'cur_stock_' + str(cur_day_count) + '_' + str(hist_day_count)
    hist_sales_col = 'hist_stock_' + str(cur_day_count) + '_' + str(hist_day_count)
    sales_ratio_col = 'stock_ratio_' + str(cur_day_count) + '_' + str(hist_day_count)

    df = df.withColumn(cur_sales_col,
                       F.when(F.col(cur_sales_col) < 0, F.lit(0)).otherwise(
                           F.col(cur_sales_col)))

    df = df.withColumn(hist_sales_col,
                       F.when(F.col(hist_sales_col) < 0, F.lit(0)).otherwise(
                           F.col(hist_sales_col)))
    '''
    df = df.withColumn(hist_sales_col, custom_divide_udf(F.col(hist_sales_col),F.lit(cur_hist_ratio)))

    df = df.withColumn(sales_ratio_col,
                       F.when(F.col(hist_sales_col) != 0,
                              F.round(F.col(cur_sales_col) / F.col(hist_sales_col),
                                      2)).otherwise(F.lit(100)))
    '''
    df = df.withColumn(sales_ratio_col,
                       custom_divide_udf(F.col(hist_sales_col), F.col(cur_sales_col), F.lit(cur_hist_ratio)))
    return df


def compute_sales_ratio(df, config_dict, cur_day_count, hist_day_count):
    # df = df.sort_values(by=[config_dict['inventory_snapshot']['dest_dict']['date']])
    cur_hist_ratio = hist_day_count / cur_day_count
    cur_sales_col = 'cur_sales_' + str(cur_day_count) + '_' + str(hist_day_count)
    hist_sales_col = 'hist_sales_' + str(cur_day_count) + '_' + str(hist_day_count)
    sales_ratio_col = 'sales_ratio_' + str(cur_day_count) + '_' + str(hist_day_count)

    df = df.withColumn(cur_sales_col,
                       F.when(F.col(cur_sales_col) < 0, F.lit(0)).otherwise(
                           F.col(cur_sales_col)))

    df = df.withColumn(hist_sales_col,
                       F.when(F.col(hist_sales_col) < 0, F.lit(0)).otherwise(
                           F.col(hist_sales_col)))
    '''
    df = df.withColumn(hist_sales_col, F.col(hist_sales_col) / F.lit(cur_hist_ratio))

    df = df.withColumn(sales_ratio_col,
                       F.when(F.col(hist_sales_col) != 0,
                              F.round(F.col(cur_sales_col) / F.col(hist_sales_col),
                                      2)).otherwise(F.lit(100)))
    '''
    df = df.withColumn(sales_ratio_col,
                       custom_divide_udf(F.col(hist_sales_col), F.col(cur_sales_col), F.lit(cur_hist_ratio)))
    return df


def compute_sell_through_rate(df, config_dict, day_count):
    df = df.withColumn('out_' + str(day_count),
                       F.when(F.col('out_' + str(day_count)) < 0, F.lit(0)).otherwise(
                           F.col('out_' + str(day_count))))
    '''
    df = df.withColumn('sell_through_rate_' + str(day_count),
                       F.when(F.col('in_' + str(day_count)) != 0,
                              F.round(F.col('out_' + str(day_count)) / F.col('in_' + str(day_count)),
                                      2)).otherwise(F.lit(100)))

    '''
    df = df.withColumn('sell_through_rate_' + str(day_count),
                       custom_ratio_udf(F.col('out_' + str(day_count)), F.col('in_' + str(day_count))))
    return df


# def custom_ratio(up_col, down_col):
#     if down_col != 0.0:
#         return round(up_col / down_col, 2)
#     else:
#         return 100.0

def custom_ratio(up_col, down_col):
    try:
        if up_col is not None and down_col is not None and down_col != 0.0:
            return round(up_col / down_col, 2)
        else:
            return 100.0
    except TypeError:
        return 100.0






custom_ratio_udf = F.udf(custom_ratio, DoubleType())


def compute_inventory_turn_over(df, config_dict, day_count):
    df = df.withColumn('itr_cogs_' + str(day_count),
                       F.when(F.col('itr_cogs_' + str(day_count)) < 0, F.lit(0)).otherwise(
                           F.col('itr_cogs_' + str(day_count))))
    df = df.withColumn('itr_avg_start_' + str(day_count),
                       F.when(F.col('itr_avg_start_' + str(day_count)) < 0, F.lit(0)).otherwise(
                           F.col('itr_avg_start_' + str(day_count))))

    df = df.withColumn('itr_avg_end_' + str(day_count),
                       F.when(F.col(config_dict['inventory_snapshot']['dest_dict']['day_end_soh_balance']) < 0,
                              F.lit(0)).otherwise(
                           F.col(config_dict['inventory_snapshot']['dest_dict']['day_end_soh_balance'])))
    df = df.withColumn('itr_avg_inv_' + str(day_count),
                       (F.col('itr_avg_start_' + str(day_count)) + F.col('itr_avg_end_' + str(day_count))) / 2)
    # df = df.withColumn('itr_ratio_' + str(day_count),
    #                   F.when(F.col('itr_avg_inv_' + str(day_count)) != 0, F.round(F.col('itr_cogs_' + str(day_count))/F.col('itr_avg_inv_' + str(day_count)), 2)).otherwise(F.lit(100)))

    df = df.withColumn('itr_ratio_' + str(day_count),
                       custom_ratio_udf(F.col('itr_cogs_' + str(day_count)), F.col('itr_avg_inv_' + str(day_count))))
    '''
    df = df.withColumn('inv_sales_ratio_' + str(day_count),
                       F.when(F.col('itr_cogs_' + str(day_count)) != 0,
                              F.round(F.col('itr_avg_inv_' + str(day_count)) / F.col('itr_cogs_' + str(day_count)),
                                      2)).otherwise(F.lit(100)))
    '''
    df = df.withColumn('inv_sales_ratio_' + str(day_count),
                       custom_ratio_udf(F.col('itr_avg_inv_' + str(day_count)), F.col('itr_cogs_' + str(day_count))))

    df = df.withColumn('inv_shrinkage_' + str(day_count),
                       F.when(F.col(config_dict['derived_fields']['dest_dict']['audit_adj_units']) < 0,
                              F.abs(config_dict['derived_fields']['dest_dict']['audit_adj_units'])).otherwise(
                           F.lit(0)))

    '''
    df = df.withColumn('inv_shrinkage_ratio_' + str(day_count),
                       F.when(F.col('itr_avg_inv_' + str(day_count)) != 0,
                              F.round(F.col('inv_shrinkage_' + str(day_count)) / F.col('itr_avg_inv_' + str(day_count)),
                                      2)).otherwise(F.lit(100)))
    '''
    df = df.withColumn('inv_shrinkage_ratio_' + str(day_count),
                       custom_ratio_udf(F.col('inv_shrinkage_' + str(day_count)),
                                        F.col('itr_avg_inv_' + str(day_count))))

    return df


def compute_shelf_life(df, config_dict, sku_col_name, loc_col_name, date_col_name):
    window_spec = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name))
    window_spec_cumulative = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name)).rowsBetween(
        Window.unboundedPreceding, Window.currentRow)
    day_begin_soh_bal = config_dict['inventory_snapshot']['dest_dict']['day_begin_soh_balance']
    day_start_intrans_bal = config_dict['inventory_snapshot']['dest_dict']['day_start_instrans_balance']
    day_end_intrans_bal = config_dict['inventory_snapshot']['dest_dict']['day_end_intras_balance']

    df = df.withColumn('CALC_CUMULATIVE_SOH',
                       F.when(F.row_number().over(window_spec) == 1,
                              (F.col(day_begin_soh_bal) + F.col('meta_CUMULATIVE_SOH'))).otherwise(
                           F.col(day_begin_soh_bal)))

    df = df.withColumn('CALC_CUMULATIVE_SOH', F.sum(F.col('CALC_CUMULATIVE_SOH')).over(window_spec_cumulative))

    df = df.withColumn('CALC_CUMULATIVE_DAILY_OUT', F.col(config_dict['sales']['dest_dict']['sales_units']) +
                       F.col(config_dict['returns']['dest_dict']['return_units']) - F.col(
        config_dict['derived_fields']['dest_dict']['audit_adj_units']))

    df = df.withColumn('CALC_CUMULATIVE_DAILY_OUT',
                       F.when(F.row_number().over(window_spec) == 1,
                              (F.col('CALC_CUMULATIVE_DAILY_OUT') + F.col('meta_CUMULATIVE_DAILY_OUT'))).otherwise(
                           F.col('CALC_CUMULATIVE_DAILY_OUT')))

    df = df.withColumn('CALC_CUMULATIVE_DAILY_OUT',
                       F.sum(F.col('CALC_CUMULATIVE_DAILY_OUT')).over(window_spec_cumulative))

    df = df.withColumn('BATCH_QTY', F.when(F.col(day_end_intrans_bal) < F.col(day_start_intrans_bal),
                                           F.col(day_start_intrans_bal) - F.col(day_end_intrans_bal)))

    df = df.withColumn('CALC_CUMULATIVE_BATCH_QTY',
                       F.when((F.row_number().over(window_spec) == 1) & F.col('BATCH_QTY').isNotNull(),
                              F.col('BATCH_QTY') + F.col('meta_CUMULATIVE_BATCH_QTY')).when(
                           (F.row_number().over(window_spec) == 1) & F.col('BATCH_QTY').isNull(),
                           F.col('meta_CUMULATIVE_BATCH_QTY')).otherwise(F.col('BATCH_QTY')))
    df = df.withColumn('CALC_CUMULATIVE_BATCH_QTY',
                       F.when(F.col('CALC_CUMULATIVE_BATCH_QTY').isNotNull(),
                              F.sum(F.col('CALC_CUMULATIVE_BATCH_QTY')).over(window_spec_cumulative)))

    df = df.withColumn('CALC_CUMULATIVE_BATCH_QTY',
                       F.last(F.col('CALC_CUMULATIVE_BATCH_QTY'), ignorenulls=True).over(window_spec))

    df = df.withColumn('CARRY_FORWARD_CUMULATIVE_SOH', F.when(F.col(day_end_intrans_bal) < F.col(day_start_intrans_bal),
                                                              F.col('CALC_CUMULATIVE_SOH')))

    df = df.withColumn('CARRY_FORWARD_CUMULATIVE_SOH',
                       F.when((F.row_number().over(window_spec) == 1) & F.col('CARRY_FORWARD_CUMULATIVE_SOH').isNull(),
                              F.col('meta_CARRY_FORWARD_CUMULATIVE_SOH')).otherwise(
                           F.col('CARRY_FORWARD_CUMULATIVE_SOH')))

    df = df.withColumn('CARRY_FORWARD_CUMULATIVE_SOH',
                       F.last(F.col('CARRY_FORWARD_CUMULATIVE_SOH'), ignorenulls=True).over(window_spec))

    df = df.withColumn('CARRY_FORWARD_CUMULATIVE_SOH',
                       F.when(F.col('CALC_CUMULATIVE_DAILY_OUT') >= F.col('CALC_CUMULATIVE_BATCH_QTY'),
                              F.lit(0)).otherwise(F.col('CARRY_FORWARD_CUMULATIVE_SOH')))

    df = df.withColumn('CALC_CUMULATIVE_SOH', F.col('CALC_CUMULATIVE_SOH') - F.col('CARRY_FORWARD_CUMULATIVE_SOH'))

    df = df.withColumn('CALC_CUMULATIVE_SOH_MIN',
                       F.when((F.row_number().over(window_spec) == 1) & (
                                   F.col('meta_REV_CUMULATIVE_SOH_MIN') < F.col('CALC_CUMULATIVE_SOH')),
                              F.col('meta_REV_CUMULATIVE_SOH_MIN')).otherwise(F.col('CALC_CUMULATIVE_SOH')))

    df = df.withColumn('CALC_CUMULATIVE_SOH_MAX',
                       F.when((F.row_number().over(window_spec) == 1) & (
                               F.col('meta_REV_CUMULATIVE_SOH_MAX') > F.col('CALC_CUMULATIVE_SOH')),
                              F.col('meta_REV_CUMULATIVE_SOH_MAX')).otherwise(F.col('CALC_CUMULATIVE_SOH')))

    df = df.withColumn('REV_CUMULATIVE_SOH_SCALE', F.round(
        (F.col('CALC_CUMULATIVE_SOH') - F.min(F.col('CALC_CUMULATIVE_SOH_MIN')).over(window_spec_cumulative)) / (
                    F.max(F.col('CALC_CUMULATIVE_SOH_MAX')).over(window_spec_cumulative) - F.min(
                F.col('CALC_CUMULATIVE_SOH_MIN')).over(window_spec_cumulative)), 2))

    return df


def custom_ratio_with_neg(up_col, down_col):
    try:
        if down_col != 0.0:
            return round(up_col / down_col, 2)
        elif up_col >= 0.0:
            return 100.0
        else:
            return -100.0
    except (TypeError, ZeroDivisionError):
        return -100.0



custom_ratio_with_neg_udf = F.udf(custom_ratio_with_neg, DoubleType())


def compute_ledger_balance(df, config_dict, sku_col_name, loc_col_name, date_col_name):
    cols_before = list(df.columns)
    window_spec = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name))
    window_spec_cumulative = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name)).rowsBetween(
        Window.unboundedPreceding, Window.currentRow)
    window_spec_7 = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name)).rowsBetween(-6, 0)
    window_spec_14 = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name)).rowsBetween(-13, 0)
    window_spec_28 = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name)).rowsBetween(-27, 0)

    df = df.withColumn('LEDGER_OUT', F.col(config_dict['sales']['dest_dict']['sales_units']) - F.col(
        config_dict['derived_fields']['dest_dict']['audit_adj_units']))

    df = df.withColumn('daily_ledger_diff', F.col('ONHAND_RECORDED') - F.col('LEDGER_OUT'))

    df = df.withColumn('daily_ledger_diff',
                       F.when(F.row_number().over(window_spec) == 1,
                              F.col('daily_ledger_diff') + F.col('meta_ledger_bal_ip_qty_eop_soh')).otherwise(
                           F.col('daily_ledger_diff')))

    df = df.withColumn('cumulative_daily_ledger_diff', F.sum(F.col('daily_ledger_diff')).over(window_spec_cumulative))

    df = df.withColumn('AVG_LEDGER_IP_QTY_EOP_SOH_7', F.avg(F.col('cumulative_daily_ledger_diff')).over(window_spec_7))
    df = df.withColumn('AVG_LEDGER_IP_QTY_EOP_SOH_14',
                       F.avg(F.col('cumulative_daily_ledger_diff')).over(window_spec_14))
    df = df.withColumn('AVG_LEDGER_IP_QTY_EOP_SOH_28',
                       F.avg(F.col('cumulative_daily_ledger_diff')).over(window_spec_28))

    df = df.withColumn('LEDGER_BAL_RATIO_7',
                       custom_ratio_with_neg_udf(F.col('AVG_LEDGER_IP_QTY_EOP_SOH_7'), F.col('AVG_IP_QTY_EOP_SOH_7')))
    df = df.withColumn('LEDGER_BAL_RATIO_14',
                       custom_ratio_with_neg_udf(F.col('AVG_LEDGER_IP_QTY_EOP_SOH_14'), F.col('AVG_IP_QTY_EOP_SOH_14')))
    df = df.withColumn('LEDGER_BAL_RATIO_28',
                       custom_ratio_with_neg_udf(F.col('AVG_LEDGER_IP_QTY_EOP_SOH_28'), F.col('AVG_IP_QTY_EOP_SOH_28')))
    '''
    df = df.withColumn('LEDGER_BAL_RATIO_7',F.when((F.col('AVG_IP_QTY_EOP_SOH_7') == 0) & (F.col('AVG_LEDGER_IP_QTY_EOP_SOH_7')  < 0), F.lit(-100)).when((F.col('AVG_IP_QTY_EOP_SOH_7') == 0) & (F.col('AVG_LEDGER_IP_QTY_EOP_SOH_7')  >= 0), F.lit(100)).otherwise(F.round(F.col('AVG_LEDGER_IP_QTY_EOP_SOH_7') / F.col('AVG_IP_QTY_EOP_SOH_7'), 2)))
    df = df.withColumn('LEDGER_BAL_RATIO_14',
                       F.when((F.col('AVG_IP_QTY_EOP_SOH_14') == 0) & (F.col('AVG_LEDGER_IP_QTY_EOP_SOH_14') < 0),
                              F.lit(-100)).when(
                           (F.col('AVG_IP_QTY_EOP_SOH_14') == 0) & (F.col('AVG_LEDGER_IP_QTY_EOP_SOH_14') >= 0),
                           F.lit(100)).otherwise(
                           F.round(F.col('AVG_LEDGER_IP_QTY_EOP_SOH_14') / F.col('AVG_IP_QTY_EOP_SOH_14'), 2)))

    df = df.withColumn('LEDGER_BAL_RATIO_28',
                       F.when((F.col('AVG_IP_QTY_EOP_SOH_28') == 0) & (F.col('AVG_LEDGER_IP_QTY_EOP_SOH_28') < 0),
                              F.lit(-100)).when(
                           (F.col('AVG_IP_QTY_EOP_SOH_28') == 0) & (F.col('AVG_LEDGER_IP_QTY_EOP_SOH_28') >= 0),
                           F.lit(100)).otherwise(
                           F.round(F.col('AVG_LEDGER_IP_QTY_EOP_SOH_28') / F.col('AVG_IP_QTY_EOP_SOH_28'), 2)))
    '''
    return df.select(*cols_before, 'LEDGER_BAL_RATIO_7', 'LEDGER_BAL_RATIO_14', 'LEDGER_BAL_RATIO_28')


def footfall_index(df):
    import pandas as pd
    df['footfall_index'] = df.groupby('DATE')[
        'footfall_scale'].transform(
        lambda x: pd.cut(x, [0, 0.1, .2, .3, .4, .5, .6, .7, .8, .9, 1.], labels=range(1, 11)))
    df['footfall_index'] = df['footfall_index'].fillna(value=1)
    return df


def compute_footfall(df, config_dict, sku_col_name, loc_col_name, date_col_name):
    df_loc = df.groupBy([loc_col_name, date_col_name]).agg(
        F.sum(config_dict['sales']['dest_dict']['sales_count']).alias('daily_trans'),
        F.first('meta_footfall_daily_min').alias('meta_footfall_daily_min'),
        F.first('meta_footfall_daily_max').alias('meta_footfall_daily_max'))

    window_spec_loc = Window.partitionBy(loc_col_name).orderBy(F.col(date_col_name))

    df_loc = df_loc.withColumn('CALC_DAILY_TRANS_MIN',
                               F.when((F.row_number().over(window_spec_loc) == 1) & (
                                       F.col('meta_footfall_daily_min') < F.col('daily_trans')),
                                      F.col('meta_footfall_daily_min')).otherwise(F.col('daily_trans')))

    df_loc = df_loc.withColumn('CALC_DAILY_TRANS_MAX',
                               F.when((F.row_number().over(window_spec_loc) == 1) & (
                                       F.col('meta_footfall_daily_max') > F.col('daily_trans')),
                                      F.col('meta_footfall_daily_max')).otherwise(F.col('daily_trans')))

    df_loc = df_loc.withColumn('footfall_scale', F.round(
        (F.col('daily_trans') - F.min(F.col('CALC_DAILY_TRANS_MIN')).over(window_spec_loc)) / (
                F.max(F.col('CALC_DAILY_TRANS_MAX')).over(window_spec_loc) - F.min(
            F.col('CALC_DAILY_TRANS_MIN')).over(window_spec_loc)), 2))

    df_loc = df_loc.withColumn('footfall_index', F.lit(None).cast(IntegerType()))
    df_loc = df_loc.drop('meta_footfall_daily_max', 'meta_footfall_daily_min')
    df_loc = df_loc.groupby(loc_col_name).applyInPandas(
        footfall_index,
        schema=StructType.fromJson(df_loc.schema.jsonValue()))
    df = df.join(df_loc, [loc_col_name, date_col_name], 'left')
    return df


def check_null_and_return_avg(values: list, expected_count):
    if len(values) < expected_count:
        return 0.0
    else:
        return sum(values) / len(values)


def udf_avg(expected_len: int):
    return F.udf(lambda x: check_null_and_return_avg(x, expected_len), DoubleType())


def compute_decay_features(meta_df, sku_col_name, loc_col_name, date_col_name):
    window_spec_roll = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name)).rowsBetween(-83, 0)
    meta_df = compute_decay_feature_internal(meta_df, sku_col_name, loc_col_name, date_col_name, 'transfer_',
                                             window_spec_roll)
    meta_df = compute_decay_feature_internal(meta_df, sku_col_name, loc_col_name, date_col_name, 'sale_',
                                             window_spec_roll)
    # meta_df = compute_decay_feature_internal(meta_df, sku_col_name, loc_col_name, date_col_name, 'meta_posadj_', window_spec_roll)
    return meta_df


def compute_decay_feature_internal(meta_df, sku_col_name, loc_col_name, date_col_name, prefix, window_spec_roll):
    meta_prefix = 'meta_' + prefix
    window_spec_decay = Window.partitionBy(sku_col_name, loc_col_name, meta_prefix + 'cur_day_count').orderBy(
        F.col(date_col_name))
    meta_df = meta_df.withColumn(meta_prefix + 'decay_decimal',
                                 F.lag(F.col(meta_prefix + 'decay_decimal'), 1).over(window_spec_decay))
    meta_df = meta_df.withColumn(meta_prefix + 'decay_decimal',
                                 F.round(udf_avg(84)(
                                     F.collect_list(F.col(meta_prefix + 'decay_decimal')).over(window_spec_roll)), 2))

    meta_df = meta_df.withColumn(prefix + 'decay_decimal', F.col(meta_prefix + 'decay_decimal'))

    return meta_df


def compute_daily(df, config_dict, sku_col_name, loc_col_name, date_col_name):
    df = df.withColumn('day_of_week', ((F.dayofweek(F.col(date_col_name)) + 5) % 7))
    df = df.withColumn(config_dict['derived_fields']['dest_dict']['audit_adj_eop_soh'],
                       F.when(F.col(config_dict['inventory_snapshot']['dest_dict']['day_end_soh_balance']) < 0,
                              F.lit(0)).otherwise(
                           F.col(config_dict['inventory_snapshot']['dest_dict']['day_end_soh_balance'])))
    df = compute_inventory_turn_over(df, config_dict, 7)
    df = compute_inventory_turn_over(df, config_dict, 14)
    df = compute_inventory_turn_over(df, config_dict, 28)
    df = compute_sell_through_rate(df, config_dict, 28)
    df = compute_sell_through_rate(df, config_dict, 56)
    df = compute_sell_through_rate(df, config_dict, 84)
    df = compute_sales_ratio(df, config_dict, 7, 28)
    df = compute_sales_ratio(df, config_dict, 28, 84)
    df = compute_stock_ratio(df, config_dict, 7, 28)
    df = compute_stock_ratio(df, config_dict, 28, 84)
    df = compute_sale_day_ratio(df, config_dict, 28)
    df = compute_sale_day_ratio(df, config_dict, 84)
    window_spec_loc = Window.partitionBy(loc_col_name, date_col_name)
    # df = compute_index_features(df, config_dict, 7, 'avg_return_', 'return_scale_', window_spec_loc)
    # df = compute_index_features(df, config_dict, 7, 'avg_sale_', 'sale_scale_', window_spec_loc)
    df = compute_index_features(df, config_dict, 7, 'avg_price_', 'price_scale_', window_spec_loc)

    # df = compute_index_features(df, config_dict, 28, 'avg_return_', 'return_scale_', window_spec_loc)
    df = compute_index_features(df, config_dict, 28, 'avg_sale_', 'sale_scale_', window_spec_loc)
    # df = compute_index_features(df, config_dict, 28, 'avg_price_', 'price_scale_', window_spec_loc)
    df = compute_out_of_stock_ratio(df, config_dict, 28)
    df = compute_out_of_stock_ratio(df, config_dict, 84)
    df = compute_shelf_life(df, config_dict, sku_col_name, loc_col_name, date_col_name)
    df = compute_footfall(df, config_dict, sku_col_name, loc_col_name, date_col_name)
    df = compute_events_flag(df, config_dict)
    return df


def generate_result_internal(spark, location_group_id, config_path, denorm_src_path, meta_src_path, dest_path,
                             historical_mode, current_date_str):
    print('Processing', location_group_id)
    total_start_time = time.time()

    # TODO: Need  to  pass this format as input, will be  handled later
    # TODO: Calculate rolling days dynamically
    rolling_min_date = fetch_past_date_string(current_date_str, 100, '%Y-%m-%d')
    read_config = TableMappingConfiguration(config_path)
    join_dest_dict = read_config.get_dest_dict('inventory_snapshot')
    sku_col_name = join_dest_dict['sku']
    loc_col_name = join_dest_dict['location']
    date_col_name = join_dest_dict['date']
    load_date_col_name = read_config.get_dest_dict('derived_fields')['load_date']

    daily_meta_columns = ['meta_CUMULATIVE_SOH', 'meta_CUMULATIVE_DAILY_OUT',
                          'meta_CARRY_FORWARD_CUMULATIVE_SOH', 'meta_CUMULATIVE_BATCH_QTY',
                          'meta_REV_CUMULATIVE_SOH_MIN', 'meta_REV_CUMULATIVE_SOH_MAX',
                          'meta_footfall_daily_min', 'meta_footfall_daily_max']

    denorm_full_path = denorm_src_path + '/LOCATION_GROUP=' + str(location_group_id) + '/'
    meta_full_path = meta_src_path + '/LOCATION_GROUP_' + str(location_group_id) + '/'

    window_spec = Window.partitionBy(sku_col_name, loc_col_name).orderBy(
        F.col(date_col_name))

    denorm_df = spark.read.parquet(denorm_full_path)
    print("denorm_df" + str(denorm_df.count()))
    # denorm_df = denorm_df.dropDuplicates(subset=[sku_col_name, loc_col_name, date_col_name]) #Commented  for Demo

    meta_df = spark.read.parquet(meta_full_path)
    print("meta_df" + str(meta_df.count()))
    all_meta_columns = []
    for each_meta_col in meta_df.columns:
        if each_meta_col.startswith('meta_'):
            all_meta_columns.append(each_meta_col)

    meta_df = meta_df.select(sku_col_name, loc_col_name, date_col_name, *all_meta_columns)
    meta_df_daily = meta_df.select(sku_col_name, loc_col_name, date_col_name, *daily_meta_columns)
    for each_daily_meta_col in daily_meta_columns:
        meta_df_daily = meta_df_daily.withColumn(each_daily_meta_col,
                                                 F.lag(F.col(each_daily_meta_col), 1).over(window_spec))

    meta_df_rolling = meta_df.select(sku_col_name, loc_col_name, date_col_name, 'meta_ledger_bal_ip_qty_eop_soh')
    meta_df_rolling = meta_df_rolling.withColumn('meta_ledger_bal_ip_qty_eop_soh',
                                                 F.lag(F.col('meta_ledger_bal_ip_qty_eop_soh'), 1).over(window_spec))

    denorm_df_rolling = denorm_df.filter(denorm_df.DATE >= rolling_min_date)
    denorm_df_daily = denorm_df.filter(denorm_df.LOAD_DATE == current_date_str)

    # Add meta column  to denorm for incremental processing
    denorm_df_daily = denorm_df_daily.join(meta_df_daily, [sku_col_name, loc_col_name, date_col_name], 'left')
    print("denorm_df_daily" + str(denorm_df_daily.count()))
    denorm_df_rolling = denorm_df_rolling.join(meta_df_rolling, [sku_col_name, loc_col_name, date_col_name], 'left')
    print("denorm_df_rolling" + str(denorm_df_rolling.count()))
    before_cols = denorm_df_daily.columns
    before_monthly = list(denorm_df_rolling.columns)
    denorm_df_rolling = compute_rolling(denorm_df_rolling, read_config.get_computed_dict(), sku_col_name, loc_col_name,
                                        date_col_name)
    print("denorm_df_rolling" + str(denorm_df_rolling.count()))
    denorm_df_rolling = compute_ledger_balance(denorm_df_rolling, read_config.get_computed_dict(), sku_col_name,
                                               loc_col_name,
                                               date_col_name)
    print("denorm_df_rolling" + str(denorm_df_rolling.count()))

    after_monthly = list(denorm_df_rolling.columns)
    monthly_columns = set(after_monthly) - set(before_monthly)
    denorm_df_rolling = denorm_df_rolling.select(sku_col_name, loc_col_name, date_col_name, *monthly_columns)
    df_daily = denorm_df_daily.join(denorm_df_rolling, [sku_col_name, loc_col_name, date_col_name], 'left')
    df_daily = compute_daily(df_daily, read_config.get_computed_dict(), sku_col_name, loc_col_name, date_col_name)
    print("df_daily" + str(df_daily.count()))
    meta_df = compute_decay_features(meta_df, sku_col_name, loc_col_name, date_col_name)

    df_daily = df_daily.join(meta_df, [sku_col_name, loc_col_name, date_col_name], 'left')
    print("df_daily" + str(df_daily.count()))
    after_cols = df_daily.columns
    # relevant_features = set(after_cols) - set(before_cols)

    relevant_features = ['inv_shrinkage_ratio_7', 'REV_CUMULATIVE_SOH_SCALE',
                         'LEDGER_BAL_RATIO_28', 'sale_scale_28', 'LEDGER_BAL_RATIO_7',
                         'price_scale_7', 'sale_day_ratio_84', 'sale_day_ratio_28', 'footfall_scale',
                         'sale_decay_decimal', 'itr_ratio_28', 'itr_ratio_7', 'sell_through_rate_56',
                         'LEDGER_BAL_RATIO_14', 'itr_ratio_14', 'sales_ratio_28_84',
                         'oos_ratio_28', 'inv_shrinkage_ratio_14', 'stock_ratio_7_28', 'sell_through_rate_84',
                         'transfer_decay_decimal', 'inv_shrinkage_ratio_28', 'categorical_shrinkage_scale_7',
                         'oos_ratio_84', 'inv_sales_ratio_7', 'stock_ratio_28_84', 'inv_sales_ratio_28',
                         'inv_sales_ratio_14', 'sell_through_rate_28', 'footfall_index', 'sales_ratio_7_28',
                         'CAT',
                         'CURDAY_IP_QTY_EOP_SOH',
                         'DEPT',
                         'LOB',
                         'audit_day_flag',
                         'day_of_week']

    print(relevant_features)

    df_daily = df_daily.select(sku_col_name, loc_col_name, date_col_name, *relevant_features)

    # if not historical_mode:
    #    df = df.filter(F.col(read_config.get_dest_dict('derived_fields')['orig_load_date']) == current_date_str)
    #    df = df.cache()
    print('Writing')
    out_file = dest_path + '/LOCATION_GROUP=' + str(location_group_id) + '/'
    df_daily.write.option("header", True).option("compression", "gzip").mode("overwrite").parquet(out_file)

    print('Total Time Taken', time.time() - total_start_time, 'Seconds')
    print(dest_path)


def generate_results(spark, location_groups, config_path, denorm_src_path, meta_src_path, dest_path, historical_mode,
                     current_date_str):
    for each_loc_group in location_groups:
        generate_result_internal(spark, each_loc_group, config_path, denorm_src_path, meta_src_path, dest_path,
                                 historical_mode, current_date_str)


def main(args=None):
    print("args", args)
    parser = argparse.ArgumentParser(description='Running DS Ready FS Subset')

    parser.add_argument(
        '--location_groups',
        dest='location_groups',
        type=str,
        required=True,
        help='comma seperated list of location group IDs to be processed')

    parser.add_argument(
        '--mapping_path',
        dest='mapping_path',
        type=str,
        required=True,
        help='GCS Location of the table mapping JSON')

    parser.add_argument(
        '--denorm_src_path',
        dest='denorm_src_path',
        type=str,
        required=True,
        help='GCS Path where the consolidated denormalized parquets are located')

    parser.add_argument(
        '--meta_src_path',
        dest='meta_src_path',
        type=str,
        required=True,
        help='GCS Path where the meta column parquets are located')

    parser.add_argument(
        '--dest_path',
        dest='dest_path',
        type=str,
        required=True,
        help='GCS Path where the daily incremental feature store parquet to be stored')

    parser.add_argument(
        '--historical_mode',
        dest='historical_mode',
        type=str,
        required=True,
        help='Y or N  indicating whether to run in historical mode or Incremental mode, Y for Historical and N for Incremental')

    parser.add_argument(
        '--current_date_str',
        dest='current_date_str',
        type=str,
        required=True,
        help='The string value  of the current date')

    args = parser.parse_args(args)

    print('args.location_groups', args.location_groups)
    print('args.mapping_path', args.mapping_path)
    print('args.denorm_src_path', args.denorm_src_path)
    print('args.meta_src_path', args.meta_src_path)
    print('args.dest_path', args.dest_path)
    print('args.historical_mode', args.historical_mode)
    print('args.current_date_str', args.current_date_str)

    historical_mode = False
    if args.historical_mode == 'Y':
        historical_mode = True

    location_groups = args.location_groups.split(',')

    spark = utils.get_spark_session()
    generate_results(spark, location_groups, args.mapping_path, args.denorm_src_path, args.meta_src_path,
                     args.dest_path, historical_mode, args.current_date_str)


if __name__ == '__main__':
    main()