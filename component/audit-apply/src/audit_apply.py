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

This is a component used to apply audit columns on denorm data generated, 

spark-submit <SPARK_OPTIONS> business_feature_store.py
  --location_groups LOCATION_GROUPS
                        comma seperated list of location group IDs to be processed
  --mapping_path MAPPING_PATH
                        GCS Location of the table mapping JSON
  --denorm_src_path DENORM_SRC_PATH
                        GCS Path where the consolidated denormalized parquets are located
  --dest_path GCS Path where the daily incremental feature store parquet to be stored
  --historical_mode HISTORICAL_MODE
                        Y or N indicating whether to run in historical mode or Incremental mode, Y for Historical and N for Incremental
  --current_date_str CURRENT_DATE_STR
                        The string value of the current date in the format %Y-%m-%d
"""

print('Starting audit_apply')


def audit_apply(spark, each_loc_group, config_path, denorm_src_path,
                dest_path, historical_mode, current_date_str):
    read_config = TableMappingConfiguration(config_path)
    denorm_full_path = denorm_src_path + '/LOCATION_GROUP=' + str(each_loc_group) + '/'
    denorm_df = spark.read.parquet(denorm_full_path)

    audit_date_col = read_config.get_dest_dict('yearly_audit')['audit_date']
    audit_units_col = read_config.get_dest_dict('yearly_audit')['audit_units']
    sku_col_name = read_config.get_dest_dict('inventory_snapshot')['sku']
    loc_col_name = read_config.get_dest_dict('inventory_snapshot')['location']
    date_col_name = read_config.get_dest_dict('inventory_snapshot')['date']

    window_spec_desc = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name).desc())
    if not historical_mode:
        print("inside not historical mode")
        # print("denorm count() >> " + str(denorm_df.count()))
        denorm_df.select("DATE", audit_date_col).show()
        current_audit_sku_loc_df = denorm_df.filter(
            (date_format(col("DATE"), "yyyy-MM-dd") == current_date_str) & (F.col(audit_date_col).isNotNull())).select(
            sku_col_name,
            loc_col_name).distinct()
        # print("current_audit_sku_loc_df filter >>>" + str(current_audit_sku_loc_df.count()))
        # current_audit_sku_loc_df = current_audit_sku_loc_df.withColumn('FILTER', F.lit(1))
        denorm_df = denorm_df.join(current_audit_sku_loc_df, [sku_col_name, loc_col_name], 'left')
        # denorm_df = denorm_df.filter(F.col('FILTER') == 1)
        # print("denorm after joining current_audit_sku_loc_df  >>>" + str(denorm_df.count()))
        # denorm_df = denorm_df.drop('FILTER')

        denorm_df = denorm_df.groupby([sku_col_name, loc_col_name]).applyInPandas(
            functools.partial(remove_duplicates, load_date_col_name=date_col_name, sku_col_name=sku_col_name,
                              loc_col_name=loc_col_name),
            schema=StructType.fromJson(denorm_df.schema.jsonValue()))
        # print("denorm after de dups >> " + str(denorm_df.count()))
    denorm_df = denorm_df.withColumn(read_config.get_dest_dict('derived_fields')['audit_day_units'],
                                     F.when(F.col(date_col_name) == F.col(audit_date_col),
                                            F.col(audit_units_col)).otherwise(F.lit(0)))
    denorm_df = denorm_df.withColumn(audit_date_col, F.last(audit_date_col, ignorenulls=True).over(window_spec_desc))
    denorm_df = denorm_df.withColumn(audit_units_col,
                                     F.last(audit_units_col, ignorenulls=True).over(window_spec_desc))

    denorm_df = denorm_df.withColumn(audit_units_col, F.when((F.col(
        read_config.get_dest_dict('inventory_snapshot')['day_start_total_balance']) + F.col(
        read_config.get_dest_dict('store_transfers')['in_transit_units']) + F.col(
        read_config.get_dest_dict('derived_fields')['dsd_transferred_units']) + F.col(audit_units_col) + F.col(
        read_config.get_dest_dict('inventory_adjustment')['adj_units']) - F.col(
        read_config.get_dest_dict('sales')['sales_units'])) == F.col(
        read_config.get_dest_dict('inventory_snapshot')['day_end_total_balance']), F.col(audit_units_col)).otherwise(
        F.lit(0)))

    denorm_df = denorm_df.withColumn(read_config.get_dest_dict('derived_fields')['audit_adj_units'],
                                     F.col(audit_units_col) + F.col(
                                         read_config.get_dest_dict('inventory_adjustment')['adj_units']))

    denorm_df = denorm_df.withColumn(read_config.get_dest_dict('derived_fields')['onhand_recorded'], F.when(
        (F.col(read_config.get_dest_dict('inventory_snapshot')['day_end_soh_balance']) + F.col(
            read_config.get_dest_dict('sales')['sales_units']) - F.col(
            read_config.get_dest_dict('derived_fields')['audit_adj_units'])) > F.col(
            read_config.get_dest_dict('inventory_snapshot')['day_begin_soh_balance']), (
                (F.col(read_config.get_dest_dict('inventory_snapshot')['day_end_soh_balance']) + F.col(
                    read_config.get_dest_dict('sales')['sales_units']) - F.col(
                    read_config.get_dest_dict('derived_fields')['audit_adj_units'])) - F.col(
            read_config.get_dest_dict('inventory_snapshot')['day_begin_soh_balance']))).otherwise(F.lit(0)))

    denorm_df = denorm_df.withColumn(read_config.get_dest_dict('derived_fields')['onhand_recorded'],
                                     F.when(F.col(
                                         read_config.get_dest_dict('derived_fields')['dsd_transferred_units']) < 0,
                                            F.col(
                                                read_config.get_dest_dict('derived_fields')['onhand_recorded']) + F.col(
                                                read_config.get_dest_dict('derived_fields')[
                                                    'dsd_transferred_units'])).otherwise(
                                         F.col(read_config.get_dest_dict('derived_fields')['onhand_recorded'])))

    denorm_df = calc_audit_cc(denorm_df, read_config)
    denorm_df = calc_auditday_eop_soh(denorm_df, read_config)
    out_file = dest_path + '/LOCATION_GROUP=' + str(each_loc_group) + '/'
    denorm_df.write.option("header", True).option("compression", "gzip").mode("overwrite").parquet(out_file)


def calc_auditday_eop_soh(df, read_config):
    df = df.withColumn(read_config.get_dest_dict('derived_fields')['audit_adj_eop_soh'], F.when(
        (F.col(read_config.get_dest_dict('yearly_audit')['audit_units']) != 0) & (
                F.col(read_config.get_dest_dict('derived_fields')['audit_day_units']) != 0),
        F.col(read_config.get_dest_dict('inventory_snapshot')['day_end_soh_balance']) - F.col(
            read_config.get_dest_dict('yearly_audit')['audit_units'])).otherwise(
        F.col(read_config.get_dest_dict('inventory_snapshot')['day_end_soh_balance'])))
    df = df.withColumn(read_config.get_dest_dict('derived_fields')['audit_adj_eop_soh'],
                       F.when(F.col(read_config.get_dest_dict('derived_fields')['audit_adj_eop_soh']) < 0,
                              F.lit(0)).otherwise(
                           F.col(read_config.get_dest_dict('derived_fields')['audit_adj_eop_soh'])))
    return df


def calc_audit_cc(df, read_config):
    audit_flag_name = read_config.get_dest_dict('yearly_audit_schedule')['audit_flag']
    audit_day_units_col = read_config.get_dest_dict('derived_fields')['audit_day_units']
    audit_date_col = read_config.get_dest_dict('yearly_audit')['audit_date']
    date_col_name = read_config.get_dest_dict('inventory_snapshot')['date']
    loc_col_name = read_config.get_dest_dict('inventory_snapshot')['location']
    sku_col_name = read_config.get_dest_dict('inventory_snapshot')['sku']

    window_spec = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name))
    window_spec_audit = Window.partitionBy(sku_col_name, loc_col_name, audit_date_col).orderBy(F.col(date_col_name))
    window_spec_desc = Window.partitionBy(sku_col_name, loc_col_name).orderBy(F.col(date_col_name).desc())

    df = df.withColumn('LAST_CC_DATE',
                       F.when(F.col(read_config.get_dest_dict('inventory_adjustment')['adj_units']) != 0,
                              F.col(date_col_name)).otherwise(None))
    df = df.withColumn('LAST_CC_DATE',
                       F.when(F.col('LAST_CC_DATE').isNull(),
                              F.last('LAST_CC_DATE', True).over(window_spec)).otherwise(
                           F.col('LAST_CC_DATE')))

    df = df.withColumn('AUDIT_PREV_CC_DATE', F.when(F.col(audit_flag_name) == 1, F.col('LAST_CC_DATE')).otherwise(None))

    df = df.withColumn('AUDIT_PREV_CC_DATE',
                       F.when((F.col(audit_flag_name) == 1) & F.col('AUDIT_PREV_CC_DATE').isNull(),
                              F.min(F.col(date_col_name)).over(window_spec_audit)).otherwise(
                           F.col('AUDIT_PREV_CC_DATE')))

    df = df.withColumn('AUDIT_PREV_CC_DATE', F.when(F.col('AUDIT_PREV_CC_DATE').isNull(),
                                                    F.last('AUDIT_PREV_CC_DATE', True).over(
                                                        window_spec_desc)).otherwise(
        F.col('AUDIT_PREV_CC_DATE')))

    df = df.withColumn('AUDIT_ADJ_FLAG',
                       F.when((F.col(audit_day_units_col) == 0) & (F.col(audit_flag_name) == 1), F.lit('N')).when(
                           (F.col(audit_day_units_col) != 0) & (F.col(audit_flag_name) == 1), F.lit('Y')).otherwise(
                           None))

    df = df.withColumn('AUDIT_ADJ_FLAG', F.when(F.col('AUDIT_ADJ_FLAG').isNull(),
                                                F.last('AUDIT_ADJ_FLAG', True).over(window_spec_desc)).otherwise(
        F.col('AUDIT_ADJ_FLAG')))

    df = df.withColumn(read_config.get_dest_dict('derived_fields')['pre_audit_cc_day_flag'],
                       F.when((F.col(date_col_name) >= F.col('AUDIT_PREV_CC_DATE')) & (
                               F.col(date_col_name) <= F.col(audit_date_col)) & (F.col('AUDIT_ADJ_FLAG') == 0) & (
                                      F.col(audit_flag_name) == 0), F.lit(1)).otherwise(F.lit(0)))

    return df


def generate_results(spark, location_groups, config_path, denorm_src_path, meta_src_path, dest_path, historical_mode,
                     current_date_str):
    for each_loc_group in location_groups:
        audit_apply(spark, each_loc_group, config_path, denorm_src_path, dest_path,
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
        help='Y or N  indicating whether to run in historical mode or Incremental mode,'
             ' Y for Historical and N for Incremental')

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
