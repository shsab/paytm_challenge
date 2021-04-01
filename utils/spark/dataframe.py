from logging import Logger

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as sf
from pyspark.sql.window import Window
from typing import Union
from functools import reduce
from pyspark.sql.types import *


def read_csv(spark: SparkSession,
             file_path: Union[list, str],
             sep: str = ',',
             header: bool = True,
             schema: Union[StructType, str] = None,
             infer_schema: bool = True,
             quote: str = '"',
             escape: str = '\\',
             encoding: str = 'utf-8',
             ignore_leading_white_space: bool = True,
             ignore_trailing_white_space: bool = True,
             null_value: str = None,
             nan_value: str = None,
             date_format: str = 'yyyy-MM-dd',
             timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss",
             mode: str = 'FAILFAST',
             column_name_of_corrupt_record: str = 'corrupt_record',
             multi_line: bool = False,
             num_partitions: int = 8
             ) -> DataFrame:
    """
    Read a csv or list of csv files and return a spark DF

    `Link Read More <https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.csv.html?highlight=read%20csv#pyspark.sql.DataFrameReader.csv>`_

    :param spark:
    :param file_path:
    :param sep:
    :param header:
    :param schema:
    :param infer_schema:
    :param quote:
    :param escape:
    :param encoding:
    :param ignore_leading_white_space:
    :param ignore_trailing_white_space:
    :param null_value:
    :param nan_value:
    :param date_format: sets the string that indicates a date format.
                        Custom date formats follow the formats at `Link datetime pattern <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.
    :param timestamp_format: sets the string that indicates a timestamp format.
                             Custom date formats follow the formats at `Link datetime pattern <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.
    :param mode:
    :param column_name_of_corrupt_record: when the mode is PERMISSIVE and it meets a corrupted record, puts the malformed string into a field configured
                                          by column_name_of_corrupt_record, and sets malformed fields to null.
                                          To keep corrupt records, an user can set a string type field named column_name_of_corrupt_record in an
                                          user-defined schema.
    :param multi_line:
    :return:
    """
    _parameters = dict(path=file_path,
                       sep=sep,
                       header=header,
                       inferSchema=infer_schema,
                       quote=quote,
                       escape=escape,
                       encoding=encoding,
                       ignoreLeadingWhiteSpace=ignore_leading_white_space,
                       ignoreTrailingWhiteSpace=ignore_trailing_white_space,
                       nullValue=null_value,
                       nanValue=nan_value,
                       dateFormat=date_format,
                       timestampFormat=timestamp_format,
                       mode=mode,
                       columnNameOfCorruptRecord=column_name_of_corrupt_record,
                       enforceSchema=False, # It is recommended to set this to false to avoid wrong entries
                       multiLine=multi_line)

    # For UTF-16 pyspark is not acting correctly if the multiLine is not set to True. Apparently, it is not going to be fixed soon.
    # https://issues.apache.org/jira/browse/SPARK-32961
    if 'utf-16' in encoding.lower():
        _parameters['multiLine'] = True
    if schema is not None:
        _parameters['schema'] = schema

    sdf = (spark.read.csv(**_parameters))
    if sdf.rdd.getNumPartitions() < num_partitions:
        sdf = sdf.repartition(num_partitions)
    return sdf


def read_excel(spark: SparkSession,
               file_path: str,
               header: bool = True,
               infer_schema: bool = True,
               treat_empty_values_as_nulls: bool = True,
               max_rows_in_memory: int = 20,
               excerpt_size: int = 10,
               timestamp_format: str = "yyyy-MM-dd HH:mm:ss"
               ) -> DataFrame:
    """

    :param spark:
    :param file_path:
    :param header:
    :param infer_schema:
    :param treat_empty_values_as_nulls:
    :param max_rows_in_memory:
    :param excerpt_size:
    :param timestamp_format:
    :return:
    """

    sdf = (spark.read.format("com.crealytics.spark.excel")
                     # .option("dataAddress", "'My Sheet'!B3:C35") # Optional, default: "A1"
                     .option("header", header) # Required
                     .option("location", file_path)
                     .option("treatEmptyValuesAsNulls", treat_empty_values_as_nulls) # Optional, default: true
                     .option("inferSchema", infer_schema) # Optional, default: false
                     # .option("addColorColumns", "true") # Optional, default: false
                     .option("timestampFormat", timestamp_format) # Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
                     .option("maxRowsInMemory", max_rows_in_memory) # Optional, default None. If set, uses a streaming reader which can help with big files
                     .option("excerptSize", excerpt_size) # Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
                     # .schema(myCustomSchema) # Optional, default: Either inferred schema, or all columns are Strings
                     # .option("workbookPassword", "pass")
                     .load(file_path))
    return sdf


def rename_columns(sdf: DataFrame,
                   new_column_names: list,
                   verbose: bool = True,
                   logger: Logger = None
               ) -> DataFrame:
    """
    Rename a spark dataframe based on given field names

    :param sdf: Spark DataFrame
    :param new_column_names: List of new names. It should provide the name for all the columns exist in DF in the same order.
    :param verbose: If set to True it will printSchema before and after
    :return:
    """
    if not isinstance(new_column_names, list):
        raise ValueError(f"The new_column_names parameter should contain a list that "
                         f"provides the new field names for all the columns available in "
                         f"the given DataFrame in the same order. "
                         f"The value provided is not a list! {type(new_column_names)}")
    if verbose:
        if logger is not None:
            logger.info(sdf.printSchema())
        else:
            sdf.printSchema()

    old_names = sdf.schema.names
    if len(old_names) != len(new_column_names):
        raise Exception(f"The new_column_names parameter should contain a list that "
                        f"provides the new field names for all the columns available in "
                        f"the given DataFrame in the same order. "
                        f"# Cols in DF: {len(old_names)} != # Names provided: {len(new_column_names)}")

    sdf = reduce(lambda data, idx: data.withColumnRenamed(old_names[idx], new_column_names[idx]), range(len(old_names)), sdf)
    if verbose:
        if logger is not None:
            logger.info(sdf.printSchema())
        else:
            sdf.printSchema()

    return sdf


def check_for_null_nan(sdf: DataFrame, show_detail: bool = False) -> DataFrame:
    """
    Check the DF for NaN and Null.
    :param sdf: DataFrame to check
    :param show_detail: If show_details is True, it will show the breakdown
                        of NaN and Nulls otherwise it returns total.
    :return:
    """
    not_supported_by_nan = ['timestamp', 'boolean']
    null_checks = [sf.count(sf.when(sf.isnull(c), c)).alias(c) for c, c_type in sdf.dtypes]
    nan_checks = [sf.count(sf.when(sf.isnan(c), c)).alias(c) if c_type not in not_supported_by_nan else sf.lit(0).alias(c) for c, c_type in sdf.dtypes]
    total_checks = [sf.count(sf.when(sf.isnull(c) | sf.isnan(c), c)).alias(c) if c_type not in not_supported_by_nan else sf.lit(0).alias(c) for c, c_type in sdf.dtypes]
    if show_detail:
        _checks = [sf.lit('Nulls').alias('type')]
        _checks.extend(null_checks)
        null_df = sdf.select(_checks)
        _checks = [sf.lit('NaNs').alias('type')]
        _checks.extend(nan_checks)
        nan_df = sdf.select(_checks)
        final_df = null_df.union(nan_df)
    else:
        _checks = [sf.lit('Total Nulls/NaNs').alias('type')]
        _checks.extend(total_checks)
        final_df = sdf.select(_checks)

    col_list = final_df.schema.names
    col_list.remove('type')
    final_df = transpose_df(sdf=final_df, columns=col_list, pivot_col='type')
    final_df = (final_df.withColumn('Nulls', final_df['Nulls'].cast(IntegerType()))
                        .withColumn('NaNs', final_df['NaNs'].cast(IntegerType())))
    # if show_detail:
    #     final_df = final_df.withColumn('Total Nulls/NaNs', sf.sum(final_df[col] for col, col_type in final_df.dtypes if col_type in ['int'])).select('Nulls', 'NaNs', 'Total Nulls/NaNs')
    return final_df


def transpose_df(sdf: DataFrame,
                 columns: list,
                 pivot_col: str = None,
                 order_by_col: str = None):
    """
    Pivots a DataFrame

    :param sdf:
    :param columns:
    :param pivot_col:
    :param order_by_col:
    :return:
    """
    if pivot_col is None and order_by_col is None:
        raise Exception(f"Both pivot_col and order_by_col cannot be None! "
                        f"The pivot_col has priority over order_by_col if both are provided.")

    if pivot_col is None:
        df = sdf.select(sf.row_number().over(Window.partitionBy().orderBy(sdf[order_by_col])).alias("columns"), '*')
        pivotCol = 'columns'
    else:
        df = sdf.select('*')
        pivotCol = pivot_col

    columnsValue = list(map(lambda x: str("'") + str(x) + str("',")  + str(x), columns))
    stackCols = ','.join(x for x in columnsValue)
    df_1 = (df.selectExpr(pivotCol, "stack(" + str(len(columns)) + "," + stackCols + ")")
               .select(pivotCol, "col0", "col1"))
    final_df = (df_1.groupBy(sf.col("col0"))
                    .pivot(pivotCol)
                    .agg(sf.concat_ws("", sf.collect_list(sf.col("col1"))))
                    .withColumnRenamed("col0", pivotCol))
    return final_df
