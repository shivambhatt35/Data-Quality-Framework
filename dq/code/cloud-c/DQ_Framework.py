import json
import re
import logging
import datetime
import time
import sys
from typing import Dict, List, Optional, NamedTuple
from datetime import datetime
from functools import reduce


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                             DoubleType, TimestampType, IntegerType)
from pyspark.sql.utils import AnalysisException
from pyspark import StorageLevel

pre_agg_filter = False
entity_column = 'sub_source'

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class FrameworkConfig:
    """
    Manages global configuration and state for the DQ Framework.

    Attributes:
        pre_agg_filter (bool): Flag for pre-aggregation filtering
        CURRENT_YEAR (int): Current year for date-based validations
        framework_config (dict): Framework-wide configuration settings
        failed_rules (list): List to track failed rule executions
        rule_run_date (str): Timestamp for the current rule execution
    """

    def __init__(self):
        self.pre_agg_filter = False
        self.CURRENT_YEAR = datetime.now().year
        self.framework_config = None
        self.failed_rules = []
        self.rule_run_date = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    def update_rule_run_date(self, run_date):
        """Updates the rule execution date if a specific date is provided."""
        if run_date is not None:
            self.rule_run_date = run_date

framework_config = FrameworkConfig()

def setup_logging():
    """
    Configures logging for the DQ Framework with appropriate format and level.
    Output is directed to stdout for Fabric notebook visibility.
    """
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        force=True
    )
    logging.info("Logging initialized for DQ Framework")


def load_framework_config(spark):
    """
    Loads framework configuration from JSON files in the Lakehouse.
    Validates and processes the configuration before making it available globally.

    Raises:
        ValueError: If config file is empty or not found
        Exception: For other configuration loading errors
    """
    try:
        # Read configuration from Lakehouse
        df = spark.read.json("Files/dq/framework_config/")

        if df.count() == 0:
            raise ValueError("Framework config file is empty or not found")

        # Extract configuration and store globally
        framework_config.framework_config = df.collect()[0].asDict()

        logging.info("Framework configuration loaded successfully")

    except Exception as e:
        logging.error(f"Error loading framework configuration: {str(e)}")
        raise

def refresh_config_on_lake(spark):

    df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("Files/dq/rule_config/rules/DQ_Rules.csv")

    df = df.withColumn("created_date", current_date()) \
           .withColumn("created_by", lit("SYSTEM")) \
           .withColumn("updated_date", current_date()) \
           .withColumn("updated_by", lit("SYSTEM")) \
           .withColumn("source", lit("MOE"))

    #df.write.format("delta").mode(config_load_mode.lower()).save("Tables/rules")
    create_table_if_not_exists("rules", df)
    merge_config_tables("rules",df,["rule_id"])

    df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("Files/dq/rule_config/threshold/DQ_Rule_Threshold.csv")

    df = df.withColumn("created_date", current_date()) \
           .withColumn("created_by", lit("SYSTEM")) \
           .withColumn("updated_date", current_date()) \
           .withColumn("updated_by", lit("SYSTEM")) \
           .withColumn("source", lit("MOE"))

    df.write.format("delta").mode("overwrite").save("Tables/rule_threshold")

    df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("Files/dq/rule_config/parameter/DQ_Rule_Parameters.csv")

    df = df.withColumn("created_date", current_date()) \
           .withColumn("created_by", lit("SYSTEM")) \
           .withColumn("updated_date", current_date()) \
           .withColumn("updated_by", lit("SYSTEM")) \
           .withColumn("source", lit("MOE"))

    #df.write.format("delta").mode(config_load_mode.lower()).save("Tables/rule_parameters")
    create_table_if_not_exists("rule_parameters", df)
    merge_config_tables("rule_parameters",df,["rule_id","system","sub_system","table_name","columns","reference_table_name","reference_columns"])


def merge_result_tables(
        target_table,
        source_df,
        keys
):
    """
    Perform a two-step process to delete and insert records in the dq_detailed_logs table.
    Deletes records matching the given keys, and inserts new records from the source DataFrame.

    :param spark: SparkSession object
    :param target_table: Name of the target table in the Lakehouse
    :param source_df: Source DataFrame containing the data to insert
    :param keys: List of columns to use as the primary key for matching
    """

    # Step 1: Load the target table and filter out records that match the keys
    target_df = spark.read.format("delta").table(target_table)

    join_conditions = [
        (col(f"target.{key}") == col(f"source.{key}")) | (col(f"target.{key}").isNull() & col(f"source.{key}").isNull())
        for key in keys]

    non_matching_df = target_df.alias("target").join(source_df.alias("source"), on=join_conditions, how="left_anti")

    non_matching_df = non_matching_df.unionByName(source_df)

    # Step 2: Write back the non-matching records (i.e., keep the remaining records)
    non_matching_df.write.format("delta").mode("overwrite").save(f"Tables/{target_table}")

def unpersist_all():
    global cached_dfs  # Use global cached_dfs
    for table_name, df in cached_dfs.items():
        if df.is_cached:
            print(f"Unpersisting DataFrame for table {table_name}")
            df.unpersist()

    # Clear the cached_dfs dictionary after unpersisting
    cached_dfs.clear()
    print("All unused cached DataFrames have been unpersisted.")


def extract_keys_values(df):
    """
    Extract key-value pairs from a DataFrame's JSON attributes and create new columns for each pair.

    Args:
        df (DataFrame): The DataFrame containing the JSON attributes to be extracted.

    Returns:
        DataFrame: The DataFrame with new columns added for each extracted key-value pair.
    """
    for i in range(4):
        key_col = f"key{i + 1}"
        value_col = f"value{i + 1}"

        # Set key column, keeping the original case from keys_array
        df = df.withColumn(key_col, expr(f"IF(size(keys_array) > {i}, keys_array[{i}], NULL)"))

        # Set value column, convert source_table_attributes to lowercase, but keep the key's case
        df = df.withColumn(value_col, expr(
            f"IF(size(keys_array) > {i}, get_json_object(lower(source_table_attributes), CONCAT('$.', lower(keys_array[{i}]))) , NULL)"))

    return df

def select_columns(df, rule_config, rule_id, rule_type, system, sub_system, domain):
    return df.select(
        lit(rule_config.get('table','')).alias("object"),
        lit(rule_config.get('dimension','')).alias("dimension"),
        lit(rule_config.get('sub_category','')).alias("category"),
        lit(rule_config.get('severity','')).alias("severity"),
        lit(system).alias("system"),
        lit(sub_system).alias("sub_system"),
        lit(domain).alias("domain"),
        lit(rule_config.get('critical_attribute','')).alias('critical_attribute'),
        lit(rule_config.get('model_integrity','')).alias('model_integrity'),
        "rule_violation_flag",
        to_json("Key_Logs", options={"ignoreNullFields": False}).alias("Key_Logs"),
        "source_table_composite_attributes_name",
        "source_table_composite_attributes_values",
        col("attributes_used_for_calculation").alias("attribute_name"),
        "value_of_attributes_used_for_calculation",
        "Violation_Details",
        lit(rule_id).alias("rule_id"),
        lit(rule_type).alias("rule_type"),
        col("year_id").cast(IntegerType()).alias("year_id"),
        col("load_date").cast(IntegerType()).alias("batch_id")
    )



def update_status(df):
    """
    Updates the 'status' column based on conditions and the state of pre-aggregation filter.

    Args:
    df (DataFrame): The Spark DataFrame to update.
    pre_agg_filter_status (bool): Indicates if pre-aggregation filtering has been applied.

    Returns:
    DataFrame: The updated DataFrame with the 'rule_violation_flag' column adjusted.
    """

    try:
        if framework_config.pre_agg_filter:
            df = df.withColumn(
                "rule_violation_flag",
                when(col("rule_violation_flag") == -1, -1)  # Preserve -1 (null) flags
                .when((col("rule_violation_flag") == 1) & (col("new_status") == 0), 0)  # Update 1 to 0 if new_status is 0
                .when(col("rule_violation_flag") == 0, 0)  # Preserve 0
                .when(col("rule_violation_flag").isNull(), col("new_status"))  # Handle null values
                .otherwise(col("new_status"))  # Use new_status for any other case
            )
        else:
            df = df.withColumn(
                "rule_violation_flag",
                when(col("rule_violation_flag").isin(-1, -2, 1), col("rule_violation_flag"))  # Preserve existing -1 and 1
                .when(col("rule_violation_flag").isin(0, None), col("new_status"))  # Update 0 and null to new_status
                .when(col("rule_violation_flag").isNull(), col("new_status"))  # Handle null values
                .otherwise(col("new_status"))  # Fallback to new_status for any other cases
            )

        # Remove the 'new_status' column as it's no longer needed after updating 'rule_violation_flag'
        df = df.drop("new_status")

        logging.info("rule_violation_flag column updated successfully.")

        return df

    except Exception as e:
        logging.error(f"Error updating rule_violation_flag column: {e}")
        raise

   # Function to union two DataFrames
def union_df(df1, df2):
    return df1.unionByName(df2)

def is_simple_field(field):
    """
    Checks if the field is a simple column name without any expressions or operations.
    """
    operators = set(' -+*/()')
    return not any((op in field) for op in operators)

def update_violation_details(df):
    """
    Updates the 'Violation_Details' column based on various conditions related to the 'rule_violation_flag' column and existing violation details.

    Args:
    df (DataFrame): The Spark DataFrame to update.

    Returns:
    DataFrame: The updated DataFrame with the 'Violation_Details' column adjusted.
    """
    try:
        logging.info("Updating Violation Details column.")

        df = df.withColumn("Violation_Details",
                           when(col("rule_violation_flag") == -1, col("Violation_Details"))
                           .when(col("rule_violation_flag") == 0, lit(""))
                           .when((col("Violation_Details") == "") & (col("New_Violation_Details") == ""), lit(""))
                           .when(col("Violation_Details").isNull() & col("New_Violation_Details").isNull(), lit(""))
                           .when((col("Violation_Details") == "") | col("Violation_Details").isNull(),
                                 col("New_Violation_Details"))
                           .when((col("New_Violation_Details") == "") | col("New_Violation_Details").isNull(),
                                 col("Violation_Details"))
                           .otherwise(concat_ws(", ",
                                                when(col("Violation_Details") != "", col("Violation_Details")),
                                                when(col("New_Violation_Details") != "", col("New_Violation_Details"))
                                                ).alias("Violation_Details"))
                           )

        # Drop the 'New_Violation_Details' column as it's no longer needed after updating 'Violation_Details'
        df = df.drop("New_Violation_Details")

        logging.info("Violation Details column updated successfully.")

        return df

    except Exception as e:
        logging.error(f"Error updating Violation Details column: {e}")
        raise

def update_key_logs(df, new_agg_fields):
    """
    Updates the 'Key_Logs' struct column by appending new aggregated fields.

    Args:
    df (DataFrame): The Spark DataFrame to update.
    new_agg_fields (list of str): List of new field names to append to 'Key_Logs'.

    Returns:
    DataFrame: The updated DataFrame with new fields added to 'Key_Logs'.
    """
    try:
        logging.info("Updating Key_Logs column with new aggregated fields.")

        # Create expressions for existing fields within 'Key_Logs'
        existing_fields_exprs = [col(f"Key_Logs.{field.name}").alias(field.name)
                                 for field in df.schema["Key_Logs"].dataType.fields]

        # Create expressions for new aggregated fields
        new_fields_exprs = [col(new_field_name) for new_field_name in new_agg_fields]

        # Update 'Key_Logs' to include both existing and new fields
        df = df.withColumn("Key_Logs", struct(*(existing_fields_exprs + new_fields_exprs)))

        logging.info("Key_Logs column updated successfully with new aggregated fields.")

        return df

    except Exception as e:
        logging.error(f"Error updating Key_Logs column with new aggregated fields: {e}")
        raise


def join_dataframes(df1: DataFrame, df2: DataFrame, how: str = 'inner', join_columns: str = None) -> DataFrame:
    try:
        if join_columns:
            # If join_columns is provided, split it by commas and strip whitespace
            common_columns = [col.strip().lower() for col in join_columns.split(',')]
            logging.info(f"Joining on user-specified columns: {common_columns}")
        else:
            # Otherwise, find the intersection of columns from both DataFrames
            df1_columns = {col.lower(): col for col in df1.columns}
            df2_columns = {col.lower(): col for col in df2.columns}
            common_columns = set(df1_columns).intersection(set(df2_columns))
            logging.info(f"No join columns specified, using common columns: {common_columns}")

        agg_df_aliased = df2.select(
            *[col(c).alias(c + "_alias") if c.lower() in common_columns else col(c) for c in df2.columns]
        )
        join_conditions = [df1[c] == agg_df_aliased[c + "_alias"] for c in common_columns]
        result_df = df1.join(agg_df_aliased, on=join_conditions, how=how)

        # Drop the aliased columns used for joining
        columns_to_drop = [c + "_alias" for c in common_columns]
        result_df = result_df.drop(*columns_to_drop)
        logging.info("DataFrames joined successfully.")

        return result_df
    except Exception as e:
        logging.error("Error occurred while joining DataFrames.", exc_info=True)
        raise e


def null_check(df, columns_to_check_for_null):
    """
    Enhances the DataFrame by checking for nulls in specified columns,
    marking the rule_violation_flag based on findings, and generating a detailed message for any violations.

    Args:
    df (DataFrame): The Spark DataFrame to be processed.
    columns_to_check_for_null (list): A list of column names to be checked for null values.

    Returns:
    DataFrame: The updated DataFrame with 'rule_violation_flag' and 'Violation_Details' columns.
    """
    try:
        logging.info(f"Starting null check for specified columns: {columns_to_check_for_null}")

        # Create null indicators using a single select statement
        null_indicators = [
            when(
                (trim(col(column_name)).isNull()) |
                (trim(col(column_name)) == "") |
                (upper(trim(col(column_name))) == "NULL"),
                lit(f"{column_name},")
            ).otherwise(lit(""))
            for column_name in columns_to_check_for_null.split(",")
        ]

        # Build the entire transformation in one go without reusing column names
        df = df.select(
            "*",  # Select all existing columns
            concat_ws("", *null_indicators).alias("Null_Indicators_Temp")  # Create a temp column for null indicators
        ).select(
            "*",  # Select all existing columns
            expr("rtrim(regexp_replace(Null_Indicators_Temp, ',$', ''))").alias("Null_Indicators_Clean")  # Trim trailing commas
        ).select(
            "*",  # Select all existing columns
            when(col("Null_Indicators_Clean") != '', lit(-1)).otherwise(lit(0)).alias("rule_violation_flag"),  # Set rule_violation_flag based on Null_Indicators
            when(col("Null_Indicators_Clean") != '',
                 concat(lit("missing value for fields: "), col("Null_Indicators_Clean"))
                 ).otherwise(None).alias("Violation_Details")  # Set violation details based on Null_Indicators
        ).drop("Null_Indicators_Temp", "Null_Indicators_Clean")  # Drop intermediate columns after processing

        logging.info("Null check completed successfully.")

        return df

    except Exception as e:
        logging.error("Error in null_check function: ", exc_info=True)
        raise


def validate_emirates_id(df: DataFrame, column_name: str, dob_column: str = '') -> DataFrame:
    """
    Generic Emirates ID validation function that adds rule_violation_flag and violation details to the DataFrame.

    Args:
    df (DataFrame): Input DataFrame containing an 'emirates_id' column.
    column_name (str): The name of the Emirates ID column.
    dob_column (str, optional): The name of the DOB column. If provided, the year in the DOB will be checked against the Emirates ID.

    Returns:
    DataFrame: Updated DataFrame with 'rule_violation_flag' and 'violation_details' columns.
    """
    try:
        # Define constants
        UPPER_LIMIT = 15
        MIN_YEAR = 1800
        CURRENT_YEAR = datetime.now().year

        logging.info("Starting Emirates ID validation")

        # Remove hyphen and spaces from Emirates ID
        df = df.withColumn("cleaned_emirates_id", regexp_replace(col(column_name), "[\\s-]+", "").cast(StringType()))
        logging.info("Cleaned Emirates ID by removing hyphens and spaces")

        # Check if Emirates ID starts with '784'
        df = df.withColumn("prefix_check", when(~col("cleaned_emirates_id").startswith("784"), lit("Invalid Emirates ID - Must start with 784")))
        logging.info("Checked if Emirates ID starts with 784")

        # Check for 15 digit length
        df = df.withColumn("length_check", when(length(col("cleaned_emirates_id")) != UPPER_LIMIT, lit("Invalid Emirates ID - Emirates ID must be 15 digits long")))
        logging.info("Checked the length of Emirates ID")

        # Extract year from Emirates ID (4th to 7th digits)
        df = df.withColumn("emirates_year", substring(col("cleaned_emirates_id"), 4, 4).cast(IntegerType()))
        logging.info("Extracted the year from Emirates ID")

        # Check if year is in a valid range
        df = df.withColumn("year_check",
            when((col("emirates_year") <= MIN_YEAR) | (col("emirates_year") >= CURRENT_YEAR),
                 lit(f"Invalid Emirates ID - Year in Emirates ID must be greater than {MIN_YEAR} and less than {CURRENT_YEAR}"))
        )
        logging.info("Checked the validity of the extracted year")

        # Check if DOB column is provided and perform the year match validation
        if dob_column:
            logging.info(f"DOB column '{dob_column}' provided, starting DOB validation")

            date_pattern_dd_mm_yyyy = r'^\d{2}-\d{2}-\d{4}$'
            date_pattern_iso = r'^\d{4}-\d{2}-\d{2}$'
            datetime_pattern_dd_mm_yyyy_hh_mm_ss = r'^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$'

            # Apply appropriate parsing based on the detected format
            df = df.withColumn("dob_converted",
                               when(regexp_extract(col(dob_column), datetime_pattern_dd_mm_yyyy_hh_mm_ss, 0) != "", to_timestamp(col(dob_column), "dd-MM-yyyy HH:mm:ss"))
                               .when(regexp_extract(col(dob_column), date_pattern_dd_mm_yyyy, 0) != "", to_date(col(dob_column), "dd-MM-yyyy"))
                               .when(regexp_extract(col(dob_column), date_pattern_iso, 0) != "", to_date(col(dob_column), "yyyy-MM-dd"))
                               .otherwise(None)
                              )
            logging.info("Converted DOB column to a valid date format")

            df = df.withColumn("dob_year", year(col("dob_converted")).cast(IntegerType()))
            logging.info("Extracted the year from the DOB column")

            df = df.withColumn("dob_year_check",
                when(col("emirates_year") != col("dob_year"),
                     lit("Invalid Emirates ID - Emirates ID year does not match DOB year"))
            )
            logging.info("Checked if Emirates ID year matches the DOB year")
        else:
            df = df.withColumn("dob_year_check", lit(""))

        # Luhn algorithm to validate the checksum
        def luhn_algorithm(eid):
            if len(eid) != UPPER_LIMIT or not eid.isdigit():  # Only check for numeric characters
                return False
            nSum = 0
            isSecond = False
            for i in range(UPPER_LIMIT - 1, -1, -1):
                d = int(eid[i])
                if isSecond:
                    d = d * 2
                nSum += d // 10
                nSum += d % 10
                isSecond = not isSecond
            return nSum % 10 == 0

        # Register Luhn UDF
        luhn_check_udf = udf(luhn_algorithm, StringType())

        # Apply Luhn algorithm check only for numeric Emirates IDs
        df = df.withColumn("luhn_check", when(~col("cleaned_emirates_id").rlike("^[0-9]*$"), lit("Invalid Emirates ID - Non-numeric characters"))
                               .otherwise(when(luhn_check_udf(col("cleaned_emirates_id")) == False, lit("Invalid Emirates ID - Invalid checksum"))))
        logging.info("Applied Luhn algorithm to validate the checksum of Emirates ID")

        # Generate violation details
        df = df.withColumn(
            "New_Violation_Details",
            expr("concat_ws(', ', prefix_check, length_check, year_check, luhn_check, dob_year_check)")
        )
        logging.info("Generated violation details")

        # Set status: 1 for failures, 0 for pass
        df = df.withColumn(
            "new_status",
            when(col("New_Violation_Details") != "", lit(1)).otherwise(lit(0))
        )
        logging.info("Set rule_violation_flag based on validation results")

        # Drop intermediate columns
        columns_to_drop = ["prefix_check", "length_check", "year_check", "luhn_check", "cleaned_emirates_id", "dob_year_check"]

        if dob_column:
            columns_to_drop.extend(["dob_year", "dob_converted"])

        df = df.drop(*columns_to_drop)
        logging.info("Dropped intermediate columns")

        logging.info("Emirates ID validation completed successfully")

        return df

    except Exception as e:
        logging.error("An error occurred during Emirates ID validation", exc_info=True)
        raise


def cast_all_columns_to_string(df):
    """
    Cast all columns in a DataFrame to string type.

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: DataFrame with all columns cast to string type
    """
    string_columns = [
        df[col_name].cast(StringType()).alias(col_name)
        if dtype != 'string' else df[col_name]
        for col_name, dtype in df.dtypes
    ]
    return df.select(*string_columns)


def read_and_persist_table(database: str, table_name: str, load_date: int, year_id: int, sub_source: str = None) -> \
Optional[DataFrame]:
    """
    Read all columns from a table in the database, persist it in memory, and cast all columns to string.

    Args:
        database (str): The name of the database.
        table_name (str): The name of the table to read.
        load_date (str): The load date for filtering the data.
        year_id (int): Specifiy if you want to run DQ for any perticular year only.
        sub_source: if sub_system = ADEK, SPEA and KHDA then pass None else pass sub_system.

    Returns:
        Optional[DataFrame]: The persisted DataFrame containing the table data, or None if the table doesn't exist.
    """
    try:
        query = f"SELECT * FROM {database}.{table_name}"
        if load_date != 0:
            query += f" WHERE load_date = '{load_date}'"

        logging.info(f"Executing query: {query}")
        logging.info(f"sub_source: {sub_source}")
        df = spark.sql(query)

        if 'year_id' in df.columns and year_id != 0:
            df = df.filter(col("year_id") == year_id)

        if 'year_id' not in df.columns:
            # Add 'year_id' column with empty values
            df = df.withColumn("year_id", lit(None))

            # Added by Saurabh
        if sub_source is None:
            logging.info(f"Inside if block of read_and_persist")
            df = df
        elif sub_source is not None and df.filter(upper(col("sub_source")) == lit(sub_source.upper())).isEmpty():
            logging.info(f"Inside elif block of read_and_persist")
            df = df.withColumn("sub_source", lit(None))
        else:
            logging.info(f"Inside else block of read_and_persist")
            df = df.filter(upper(col("sub_source")) == lit(sub_source.upper()))

        # Cast all columns to string
        df = cast_all_columns_to_string(df)
        # Persist the DataFrame
        df = df.persist(StorageLevel.MEMORY_AND_DISK)

        logging.info(f"Successfully read and persisted table: {database}.{table_name}")

        return df

    except AnalysisException as e:
        logging.error(f"Table {database}.{table_name} does not exist: {e}")
        return None
    except Exception as e:
        logging.error(f"An error occurred while reading table {database}.{table_name}: {str(e)}")
        return None


def create_table_if_not_exists(table_name, df):
    """
    Create a table if it doesn't exist in the current database.

    Args:
        table_name (str): Name of the table to check/create
        df (DataFrame): DataFrame containing the schema to use
    """
    try:
        table_exists = spark.catalog.tableExists(table_name)
        logging.info(f"Checking if Table {table_name} exists.....")

        if not table_exists:
            logging.info(f"Table {table_name} does not exist. Creating it.")
            df.limit(0).write.format("delta").saveAsTable(table_name)
            logging.info(f"Table {table_name} created successfully.")
        else:
            logging.info(f"Table {table_name} already exists. Skipping creation.")

    except Exception as e:
        logging.error(f"Error checking/creating table {table_name}: {str(e)}")
        raise


def merge_config_tables(target_table, source_df, keys):
    """
    Merge source DataFrame into target table based on specified keys.

    Args:
        target_table (str): Name of the target table
        source_df (DataFrame): Source DataFrame containing the data to merge
        keys (list): List of columns to use as the primary key for matching
    """
    update_columns = [col_name for col_name in source_df.columns if col_name not in keys]
    source_df = source_df.dropDuplicates(keys)
    source_df.createOrReplaceTempView("source_view")

    on_conditions = []
    for key in keys:
        condition = f"(target.{key} = source.{key} OR (target.{key} IS NULL AND source.{key} IS NULL))"
        on_conditions.append(condition)
    on_condition = " AND ".join(on_conditions)

    columns = ", ".join(source_df.columns)
    values = ", ".join([f"source.{col}" for col in source_df.columns])

    merge_sql = f"""
    MERGE INTO {target_table} AS target
    USING source_view AS source
    ON {on_condition}
    WHEN NOT MATCHED THEN
        INSERT ({columns})
        VALUES ({values})
    """

    spark.sql(merge_sql)


def merge_logs_tables(target_table, source_df, keys):
    """
    Perform a MERGE operation on a target table using a source DataFrame.

    :param target_table: Name of the target table in the Lakehouse
    :param source_df: Source DataFrame containing the data to merge
    :param keys: List of columns to use as the primary key for matching
    """
    # Get all column names from the source DataFrame excluding keys
    update_columns = [col_name for col_name in source_df.columns if col_name not in keys]

    # Remove duplicate rows based on keys
    source_df = source_df.dropDuplicates(keys)

    # Create temporary view for the source DataFrame
    source_df.createOrReplaceTempView("source_view")

    # Construct the ON condition for the MERGE statement
    on_condition = " AND ".join([f"target.{key} = source.{key}" for key in keys])

    # Construct the SET clause for the UPDATE operation
    set_clause = ", ".join([f"target.{col} = source.{col}" for col in update_columns])

    # Construct the column list for the INSERT operation
    columns = ", ".join(source_df.columns)
    values = ", ".join([f"source.{col}" for col in source_df.columns])

    # Generate the MERGE SQL statement
    merge_sql = f"""
    MERGE INTO {target_table} AS target
    USING source_view AS source
    ON {on_condition}
    WHEN MATCHED THEN
        UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
        INSERT ({columns})
        VALUES ({values})
    """

    # Execute the MERGE SQL statement
    spark.sql(merge_sql)
    logging.info("Merge operation completed.")


def load_rule_configurations(rule_type, rules_to_be_executed, systems_to_be_executed,
                             sub_system_to_be_executed, domain_to_be_executed,
                             tables_to_be_executed, lakehouse_layer,
                             tables_to_be_excluded=''):
    """
    Load and filter rule configurations and parameters based on specified criteria.

    Args:
        rule_type (str): The type of rules to be executed
        rules_to_be_executed (str): Comma-separated list of rules or 'all'
        systems_to_be_executed (str): Systems to be executed or 'all'
        sub_system_to_be_executed (str): Sub-systems to be executed or 'all'
        domain_to_be_executed (str): Domains to be executed or 'all'
        tables_to_be_executed (str): Tables to be executed or 'all'
        lakehouse_layer (str): Layer in the lakehouse ('silver' or 'bronze')
        tables_to_be_excluded (str): Tables to exclude from execution

    Returns:
        tuple: Filtered DataFrames for rules, thresholds, and parameters
    """
    try:
        logging.info("Started Loading Rule and Parameter Configs.")

        # Load base rules
        rules = spark.sql("select * from rules")
        rules = rules.filter(col("is_active") == 1)

        if rule_type != 'all':
            rules = rules.filter(col("rule_type") == rule_type)

        # Load rule thresholds
        rule_threshold = spark.sql("select * from rule_threshold")
        rule_threshold = rule_threshold.filter(col("is_active") == 1)

        # Load rule parameters based on rule type
        if rule_type == 'all' or rule_type == 'Data Quality':
            rule_parameters = spark.sql("select * from rule_parameters")
            rule_parameters = rule_parameters.filter(col("is_active") == 1)
        else:
            rule_parameters = None

        # Apply rule type filters
        rules = rules.filter(col("rule_type") == rule_type)
        if rule_parameters is not None:
            rule_parameters = rule_parameters.filter(col("rule_type") == rule_type)

        # Filter by specific rules if provided
        if rules_to_be_executed.lower() != 'all':
            list_of_rules = rules_to_be_executed.split(",")
            rules = rules.filter(col("rule_id").isin(list_of_rules))
            rule_threshold = rule_threshold.filter(col("rule_id").isin(list_of_rules))
            if rule_parameters is not None:
                rule_parameters = rule_parameters.filter(col("rule_id").isin(list_of_rules))

        # Filter by systems
        if systems_to_be_executed.lower() != 'all':
            systems_list = [s.lower() for s in systems_to_be_executed.split(",")]
            if rule_parameters is not None:
                rule_parameters = rule_parameters.filter(
                    lower(col("system")).isin(systems_list))

        # Filter by sub-systems
        if sub_system_to_be_executed.lower() != 'all':
            sub_systems_list = [s.lower() for s in sub_system_to_be_executed.split(",")]
            if rule_parameters is not None:
                rule_parameters = rule_parameters.filter(
                    lower(col("sub_system")).isin(sub_systems_list))

        # Filter by domains
        if domain_to_be_executed.lower() != 'all':
            domains_list = [s.lower() for s in domain_to_be_executed.split(",")]
            if rule_parameters is not None:
                rule_parameters = rule_parameters.filter(
                    lower(col("domain")).isin(domains_list))

        # Filter by tables
        if tables_to_be_executed.lower() != 'all':
            tables_list = [s.lower() for s in tables_to_be_executed.split(",")]
            if rule_parameters is not None:
                rule_parameters = rule_parameters.filter(
                    lower(col("table_name")).isin(tables_list))

        # Exclude specified tables
        if tables_to_be_excluded:
            excluded_tables = [s.lower() for s in tables_to_be_excluded.split(",")]
            if rule_parameters is not None:
                rule_parameters = rule_parameters.filter(
                    ~lower(col("table_name")).isin(excluded_tables))

        # Process rule parameters if they exist
        if rule_parameters is not None:
            # Apply lakehouse layer filtering
            if lakehouse_layer.lower() == 'silver':
                rule_parameters = rule_parameters.filter(
                    lower(col("table_name")).like('%silver%')
                )
            elif lakehouse_layer.lower() == 'bronze':
                rule_parameters = rule_parameters.filter(
                    lower(col("table_name")).like('%bnz%')
                )

            # Check mandatory fields
            mandatory_fields = ["database_name", "table_name", "columns"]
            missing_fields_filter = (
                    (col("database_name").isNull() | (trim(col("database_name")) == "")) |
                    (col("table_name").isNull() | (trim(col("table_name")) == "")) |
                    (col("columns").isNull() | (trim(col("columns")) == ""))
            )

            # Process missing mandatory fields
            missing_fields_df = rule_parameters.filter(missing_fields_filter)
            framework_config.failed_rules += [
                (row["rule_id"], "", f"Mandatory fields {', '.join(mandatory_fields)} are missing")
                for row in missing_fields_df.collect()
            ]
            rule_parameters = rule_parameters.filter(~missing_fields_filter)

            # Additional checks for reference rules (DQ_003 and DQ_004)
            ref_field_rules = ["DQ_003", "DQ_004"]
            ref_fields_filter = (
                    col("rule_id").isin(ref_field_rules) &
                    ((col("reference_database").isNull() | (trim(col("reference_database")) == "")) |
                     (col("reference_table_name").isNull() | (trim(col("reference_table_name")) == "")) |
                     (col("reference_columns").isNull() | (trim(col("reference_columns")) == "")))
            )

            # Process missing reference fields
            ref_missing_fields_df = rule_parameters.filter(ref_fields_filter)
            framework_config.failed_rules += [
                (row["rule_id"], "consistency, integrity check",
                 f"Reference fields are missing: reference_database, reference_table_name, reference_columns, for {row['table_name']}")
                for row in ref_missing_fields_df.collect()
            ]
            rule_parameters = rule_parameters.filter(~ref_fields_filter)

        logging.info("Rule configurations loaded successfully.")
        return rules, rule_threshold, rule_parameters

    except Exception as e:
        logging.error(f"Error loading rule configurations: {str(e)}")
        raise

def read_rule_configuration(rule_json_path: str) -> str:
    """
    Read and return the JSON configuration for a rule from the given path.
    """
    return spark.read.text(rule_json_path).agg(concat_ws(" ", collect_list("value"))).collect()[0][0]


class PreparedRule(NamedTuple):
    rule_id: str
    rule_name: str
    rule_description: str
    rule_type: str
    system: str
    sub_system: str
    domain: str
    json_config: str
    threshold_string: str
    columns: str
    critical_attribute: str
    model_integrity: str


def prepare_rules_for_table(rules: DataFrame, rule_parameters: DataFrame, table_name: str) -> List[PreparedRule]:
    """
    Prepare all rules to be executed for a specific table.

    Args:
        rules (DataFrame): DataFrame containing all rules.
        rule_parameters (DataFrame): DataFrame containing rule parameters.
        table_name (str): The name of the table being processed.

    Returns:
        List[PreparedRule]: A list of prepared rules for the table.
    """
    # Join rules and rule_parameters, prioritizing system, sub_system, and domain from rule_parameters
    table_rules = rules.join(
        rule_parameters.filter(col("table_name") == table_name),
        on="rule_id",
        how="inner"
    ).select(
        *[rules[c] for c in rules.columns if c not in ["system", "sub_system", "domain"]],
        # Select all columns from rules except system, sub_system, and domain
        rule_parameters.system,
        rule_parameters.sub_system,
        rule_parameters.domain,
        # Select other columns from rule_parameters that don't exist in rules
        *[rule_parameters[c] for c in rule_parameters.columns if
          c not in rules.columns and c not in ["system", "sub_system", "domain"]]
    ).dropDuplicates()  # Drop duplicates to avoid reprocessing same rules

    prepared_rules = []

    for rule_row in table_rules.collect():
        json_string = read_rule_configuration(rule_row['rule_json_path'])

        if rule_row['is_template'] == 1:
            # Prepare parameters for template
            system = rule_row['system']
            sub_system = rule_row['sub_system']
            domain = rule_row['domain']
            critical_attributes = rule_row['critical_attribute'] if rule_row['critical_attribute'] else ''
            model_integrities = rule_row['model_integrity'] if rule_row['model_integrity'] else ''

            # Handle columns consistently for all rules
            columns = [col.strip() for col in rule_row['columns'].split(",")] if rule_row['columns'] else []

            if rule_row['rule_id'] == 'DQ_005' or rule_row['rule_name'] == 'Duplicate Check':
                columns = [rule_row['columns'].strip()]
                paired_columns = zip(columns, [critical_attributes], [model_integrities])
            else:
                # For other rules, we split critical attributes and model integrities if they're provided
                columns = rule_row['columns'].split(",") if rule_row['columns'] else []
                critical_attributes = critical_attributes.split(",") if critical_attributes else [''] * len(columns)
                model_integrities = model_integrities.split(",") if model_integrities else [''] * len(columns)
                paired_columns = zip(columns, critical_attributes, model_integrities)

            for column, critical_attribute, model_integrity in paired_columns:
                column = column.strip()
                json_config = generate_template_config(json_string, rule_row, column, critical_attribute,
                                                       model_integrity)

                prepared_rule = PreparedRule(
                    rule_id=rule_row['rule_id'],
                    rule_name=rule_row['rule_name'],
                    rule_description=rule_row['rule_description'],
                    rule_type=rule_row['rule_type'],
                    system=system,
                    sub_system=sub_system,
                    domain=domain,
                    json_config=json_config,
                    threshold_string=rule_row['thresholdvalue'] if rule_row['thresholdvalue'] not in (
                    None, "null") else '{}',
                    columns=column,
                    critical_attribute=critical_attribute,
                    model_integrity=model_integrity
                )
                prepared_rules.append(prepared_rule)
        else:
            prepared_rule = PreparedRule(
                rule_id=rule_row['rule_id'],
                rule_name=rule_row['rule_name'],
                rule_description=rule_row['rule_description'],
                rule_type=rule_row['rule_type'],
                system=rule_row['system'],  # Assuming system is available from rule_row
                sub_system=rule_row['sub_system'],
                domain=rule_row['domain'],
                json_config=json_string,
                threshold_string=rule_row['thresholdvalue'] if rule_row['thresholdvalue'] not in (
                None, "null") else '{}',
                columns=rule_row['columns'],
                critical_attribute=rule_row['critical_attribute'],
                model_integrity=rule_row['model_integrity']
            )
            prepared_rules.append(prepared_rule)

    return prepared_rules


def prepare_rules(rule_engine, rule_threshold):
    """
    Prepare rules by joining rule engine and threshold configurations.

    Args:
        rule_engine (DataFrame): Rule engine configurations
        rule_threshold (DataFrame): Rule threshold configurations

    Returns:
        DataFrame: Combined and prepared rules
    """
    return join_dataframes(
        rule_engine,
        rule_threshold,
        "left_outer",
        join_columns='rule_id,domain,system,sub_system,rule_type'
    ).select(
        "rule_id", "rule_name", "rule_description", "domain", "system",
        "sub_system", "rule_json_path", "thresholdvalue", "is_template", "rule_type"
    )

def get_list_of_special_functions():
    """Get list of special aggregation functions used in data processing."""
    return ["count_distinct", "countdistinct", "approx_count_distinct",
            "collect_list", "collect_set", "first", "last"]


def generate_json_config(template, parameters):
    """
    Replace placeholders in JSON template with actual parameters.

    Args:
        template (str): JSON template string
        parameters (dict): Parameter values for substitution

    Returns:
        str: Processed JSON configuration
    """
    for key, value in parameters.items():
        if isinstance(value, (list, dict)):
            value_json = json.dumps(value)
            first_element = value[0] if isinstance(value, list) else value
            template = re.sub(f'"\${key}"', value_json, template)
            template = re.sub(rf'\(\${key}', f'({first_element}', template)
        else:
            value = str(value)
            template = template.replace(f"${key}", value)

    logging.info(f"Generated template JSON: {template}")
    return template


def generate_template_config(rule, param_row, column, critical_attribute, model_integrity):
    """
    Generate a template configuration for a given rule based on the provided parameters.

    Args:
        rule (str): The rule for which the configuration is being generated.
        param_row (dict): A dictionary containing parameter values for the configuration.
        column (str): A comma-separated string of columns.
        critical_attribute (str): A critical attribute for the rule.
        model_integrity (str): Model integrity information for the rule.

    Returns:
        dict: A JSON configuration generated for the given rule and parameters.
    """
    column = column.split(',')
    composite_columns = param_row['composite_columns'].split(',') if param_row['composite_columns'] else ''
    reference_database = param_row['reference_database']
    reference_table_name = param_row['reference_table_name']
    reference_columns = param_row['reference_columns'].split(",") if param_row['reference_columns'] else ''
    params = {
        'database': param_row['database_name'],
        'table': param_row['table_name'],
        'columns': column,
        'composite_columns': composite_columns,
        'critical_attribute': critical_attribute,
        'model_integrity': model_integrity
    }

    if reference_database is not None:
        params.update({
            'reference_database': reference_database,
            'reference_table_name': reference_table_name,
            'reference_columns': reference_columns
        })

    return generate_json_config(rule, params)

def build_condition_str(df, condition, parameters, index):
    """
    Constructs a string representation of a condition and a detailed message for violation logging.

    Args:
    condition (dict): Condition dictionary.
    parameters (dict): Parameters dictionary for value substitution.
    index (int): Condition index for unique naming.

    Returns:
    tuple: A tuple containing the condition string and the violation detail string.
    """
    field = condition['field']
    if isinstance(field, list):
        field = field[0]
    operation = condition['operation']
    exclude_flag = condition['exclude_flag'] if 'exclude_flag' in condition else 0
    # Handle regular expression matching
    if operation == "REGEX":
        non_printable_pattern = r"[^\u0600-\u06FF-\u0621-\u064A-\u0669 -~]"
        pattern = condition['pattern'].replace('\\\\', '\\')  # Assume 'pattern' key holds the regex pattern
        #pattern = condition['pattern']
        #condition_str = f"trim(regexp_replace({field},'{non_printable_pattern}','')) rlike '{pattern}'"
        is_text_pattern = bool(re.search(r'[\u0600-\u06FF-\u0621-\u064A-\u0669a-zA-Z]', pattern))

        # Construct condition string based on pattern type
        if is_text_pattern:
            # No non-printable character removal for Arabic/English text checks
            condition_str = f"trim({field}) rlike '{pattern}'"
        else:
            # Apply non-printable character removal for other types (e.g., dates, numbers)
            condition_str = f"trim(regexp_replace({field}, '{non_printable_pattern}', '')) rlike '{pattern}'"
        logging.info(f"condition_str:{condition_str}")
        #condition_str = f"trim({field}) rlike '{pattern}'"
        detail_str = f"Regex pattern violated in {field}: {pattern}"
        if exclude_flag:
           condition_str = f"NOT ({condition_str})"
        return condition_str, detail_str
    value = replace_parameters_with_values(condition['param'],parameters) if 'param' in condition else \
            condition['value']
    detail_str = f"Condition violated: {field} {operation} {value}"
    # Check if the field is of StringType and adjust for case-insensitive comparison
    if is_simple_field(field):
        field_lower = field.lower()
        if any(f.name.lower() == field_lower and isinstance(f.dataType, StringType) for f in df.schema.fields):
            # Normalize case and trim both field and value
            field = f"lower(trim({field}))"
            if isinstance(value, str):
                # If the value is a string, lower and trim it
                if "'" in value or value.upper() == 'NULL':
                    if value.upper() != 'NULL':  # Ensure it only tries to process non-null values
                        value = value.lower().strip()
                else:
                    value = f"lower(trim({value}))"
    condition_str = f"{field} {operation} {value}"
    if exclude_flag:
           condition_str = f"NOT ({condition_str})"
    logging.info(f"condition_str:{condition_str}")
    return condition_str, detail_str


def replace_parameters_with_values(expression, threshold_values):
    """
    Replace parameter placeholders with their actual values.

    Args:
        expression (str): Expression containing parameters
        threshold_values (dict): Parameter values

    Returns:
        str: Expression with parameters replaced
    """
    parts = expression.split(' ')
    modified_parts = [str(threshold_values.get(part, part)) for part in parts]
    return ' '.join(modified_parts)


def apply_filter(df, filter_conditions, parameters):
    """
    Applies filter conditions to the DataFrame, including handling of subconditions
    within group_logic, and appends detailed violation messages.

    Args:
    df (DataFrame): The Spark DataFrame to apply filters on.
    filter_conditions (list): List of dictionaries representing filter conditions.
    parameters (dict): Dictionary of parameters used in filtering expressions.

    Returns:
    DataFrame: The DataFrame updated with a 'Violation_Details' column.
    """
    try:
        logging.info("Applying filter conditions with detailed subcondition logging.")
        all_detail_columns = []
        filter_expression_parts = []

        for i, condition in enumerate(filter_conditions):
            if 'logical_operator' in condition:
                filter_expression_parts.append(f" {condition['logical_operator']} ")
                continue

            met_column = f"Condition{i + 1}_Met"
            detail_column = f"Condition{i + 1}_Detail"

            if 'group_logic' in condition:
                group_conditions = []
                sub_details = []
                for j, sub_condition in enumerate(condition['group_logic']):
                    if 'logical_operator' in sub_condition:
                        group_conditions.append(f" {sub_condition['logical_operator']} ")
                        continue

                    sub_met_column = f"Group{i + 1}_SubCondition{j + 1}_Met"
                    sub_detail_column = f"Group{i + 1}_SubCondition{j + 1}_Detail"

                    condition_str, violation_msg = build_condition_str(df, sub_condition, parameters, j + 1)

                    group_conditions.append(condition_str)
                    sub_details.append(when(expr(condition_str), lit(violation_msg)).otherwise(None).alias(sub_detail_column))

                group_condition_str = " ".join(group_conditions)
                filter_expression_parts.append(f"({group_condition_str})")

                # Prepare the detail columns for the group
                df = df.select(
                    "*",
                    expr(group_condition_str).alias(met_column),
                    concat_ws(", ", *[col(c) for c in sub_details]).alias(detail_column)
                )
                all_detail_columns.append(detail_column)
            else:
                condition_str, violation_msg = build_condition_str(df, condition, parameters, i + 1)
                filter_expression_parts.append(condition_str)

                # Add the condition and details
                df = df.select(
                    "*",
                    expr(condition_str).alias(met_column),
                    when(col(met_column), lit(violation_msg)).otherwise(None).alias(detail_column)
                )
                all_detail_columns.append(detail_column)

        # Combine individual violation details into one column
        df = df.select(
            "*",
            concat_ws(", ", *[col(c) for c in all_detail_columns]).alias("New_Violation_Details"),
            expr(" ".join(filter_expression_parts)).alias("temp_new_status")
        )

        # Final status adjustment, avoiding ambiguity
        df = df.withColumn(
            "new_status",
            when(col("temp_new_status") == True, lit(1))
            .when(col("temp_new_status") == False, lit(0))
            .when(col("temp_new_status").isNull(), lit(0))
            .otherwise(lit(-2))
        ).drop("temp_new_status")
        # Update status and violation details
        df = update_status(df)
        df = update_violation_details(df)
        # Drop the temporary detail columns used for intermediate processing
        drop_columns = all_detail_columns
        df = df.drop(*drop_columns)

        logging.info("Completed applying filter conditions with subcondition details.")
        return df

    except Exception as e:
        logging.error("Failed to apply filter conditions with subconditions.", exc_info=True)
        raise

def get_column_data_type(df, field_name):
    """
    Get the data type of a column in a DataFrame in a case-insensitive manner.

    Args:
    df (DataFrame): The Spark DataFrame to search.
    field_name (str): The name of the column (case-insensitive).

    Returns:
    DataType: The data type of the column, or None if not found.
    """
    field_name_lower = field_name.lower()
    for field in df.schema.fields:
        if field.name.lower() == field_name_lower:
            return field.dataType
    return None

def parse_configurations(rule_config_json, threshold_values):
    """Parse and extract configurations from JSON inputs."""
    rule_config = json.loads(rule_config_json)
    parameters = json.loads(threshold_values)
    return rule_config, parameters

def get_columns_used_for_calculation(rule_config, df):
    """Determine columns used for calculations."""
    columns_to_select = rule_config.get('columns', '*')
    if '*' not in columns_to_select:
        if isinstance(columns_to_select, str):
            columns_used_for_calculation = ','.join([col.strip() for col in columns_to_select.split(",")])
            return columns_used_for_calculation #columns_to_select
        return ','.join(columns_to_select)
    return ','.join(df.columns)


def add_key_logs_and_composite_attributes(df, rule_config, columns_used_for_calculation):
    """Add Key Logs and Composite Attributes to DataFrame."""
    df = df.withColumn("Key_Logs", struct(*df.columns))

    if 'composite_columns' in rule_config:
        composite_columns = rule_config['composite_columns']
        df = df.withColumn(
            "source_table_composite_attributes_name",
            concat_ws('-', *[lit(c) for c in composite_columns])
        ).withColumn(
            "source_table_composite_attributes_values",
            concat_ws('||', *[trim(col(c)) for c in composite_columns])
        )
    else:
        df = df.withColumn("source_table_composite_attributes_name", lit(None)) \
            .withColumn("source_table_composite_attributes_values", lit(None))

    df = df.withColumn("attributes_used_for_calculation", lit(columns_used_for_calculation))
    if isinstance(columns_used_for_calculation, str):
        column_list = [col.strip() for col in columns_used_for_calculation.split(",")]
    else:
        column_list = columns_used_for_calculation

    df = df.withColumn(
        "value_of_attributes_used_for_calculation",
        concat_ws(',', *[col(c) for c in column_list])
    )


def perform_null_check(df, columns_used_for_calculation):
    """Perform null check and separate flagged records."""
    df = null_check(df, columns_used_for_calculation)
    null_flagged_records = df.filter(col("rule_violation_flag") == -1)
    df = df.filter(col("rule_violation_flag") != -1)
    return df, null_flagged_records


def validate_emirates_id_logic(df, rule_config, columns_used_for_calculation):
    """Validate Emirates ID format and handle DOB column if required."""
    dob_column = None #rule_config.get('dob_column', '')
    if dob_column is None or dob_column == "" :
        df = validate_emirates_id(df,columns_used_for_calculation)
    else:
        df = validate_emirates_id(df,columns_used_for_calculation,dob_column)
    return update_status(update_violation_details(df))


def apply_pre_aggregation_filter(df, rule_config, parameters):
    """Apply filter conditions before aggregation."""
    return apply_filter(df, rule_config['filter_conditions'], parameters)


def apply_join_conditions(df, rule_config):
    """Apply join conditions from the rule configuration."""
    join_config = rule_config['join_conditions']
    join_table = spark.sql(f"select * from {join_config['joining_table']['name']}")
    join_columns = join_config['join_columns']
    join_type = join_config['type']
    return df.join(join_table, join_columns, join_type)


def apply_aggregations(df, rule_config, parameters, null_flagged_records):
    """Apply aggregations defined in the rule configuration."""
    new_agg_fields = []
    list_of_special_spark_functions = get_list_of_special_functions()
    if framework_config.pre_agg_filter:
        applicable_data = df.filter(col("rule_violation_flag") == 1)
    else:
        applicable_data = df.filter(col("rule_violation_flag").isin([0, 1]))
    non_applicable_data = df.subtract(applicable_data)

    # Function to get case-insensitive column name
    def get_case_insensitive_column(df, column_name):
        return next((col for col in df.columns if col.lower() == column_name.lower()), column_name)

    for aggregation in rule_config['aggregations']:
        group_by_columns = aggregation.get('group_by', [])
        if isinstance(group_by_columns, str):
            group_by_columns = group_by_columns.split(',')
        group_by_columns = [get_case_insensitive_column(df, col.strip()) for col in group_by_columns]

        calculations = aggregation.get('calculations', [])
        # Construct aggregation expressions
        agg_expressions = []
        for calc in calculations:
            # Constructing condition expression if exists
            filter_expression = ""
            if 'conditions' in calc:
                for condition in calc['conditions']:
                    if 'logical_operator' in condition:
                        filter_expression += f" {condition['logical_operator']} "
                    else:
                        field = get_case_insensitive_column(df, condition['field'])
                        operation = condition['operation']
                        value = replace_parameters_with_values(condition['param'],
                                                               parameters) if 'param' in condition else condition[
                            'value']
                        if is_simple_field(field):
                            if isinstance(df.schema[field].dataType, StringType):
                                # Normalize case and trim both field and value
                                field = f"lower(trim({field}))"
                                if isinstance(value, str):
                                    # If the value is a string, lower and trim it
                                    value = value.lower().strip()
                        filter_expression += f"{field} {operation} {value}"

            column = calc['column']
            if isinstance(column, list): column = column[0]
            column = get_case_insensitive_column(df, column)

            if calc['aggregation_function'].lower() in list_of_special_spark_functions:
                spark_function = getattr(F, calc['aggregation_function'].lower())
                agg_expr = spark_function(column).alias(calc['alias'])
            else:
                agg_expr = expr(f"{calc['aggregation_function'].upper()}({column}) AS {calc['alias']}")
            new_agg_fields.append(calc['alias'])

            if filter_expression != "":
                agg_df = applicable_data.filter(expr(filter_expression)).groupBy(*group_by_columns).agg(agg_expr)
            else:
                agg_df = applicable_data.groupBy(*group_by_columns).agg(agg_expr)
            applicable_data = join_dataframes(applicable_data, agg_df, 'left')

    for new_agg_field in new_agg_fields:
        non_applicable_data = non_applicable_data.withColumn(new_agg_field, lit(None))

    df = applicable_data.unionByName(non_applicable_data)
    return update_key_logs(df, new_agg_fields)

def apply_having_conditions(df, rule_config, parameters, null_flagged_records):
    """Apply having conditions after aggregation."""
    filter_conditions = rule_config['having']
    df = apply_filter(df, filter_conditions, parameters)
    if  null_flagged_records is not None:
        new_agg_fields = []
        for i, condition in enumerate(filter_conditions):
            aggreated_column = condition['field']
            new_agg_fields.append(aggreated_column)
            null_flagged_records = null_flagged_records.withColumn(aggreated_column,lit(0))
            null_flagged_records = update_key_logs(null_flagged_records, new_agg_fields)

    return df, null_flagged_records


def apply_reference_conditions(df, rule_config, parameters):
    """Apply reference conditions from the rule configuration."""
    applicable_data = df.filter(col("rule_violation_flag").isin([0, 1]))
    non_applicable_data = df.subtract(applicable_data)

    for ref_condition in rule_config['reference_conditions']:
        ref_db = ref_condition["reference_table"]["reference_database"]
        ref_table = ref_condition["reference_table"]["reference_table_name"]
        ref_columns = ref_condition["reference_columns"]

        ref_columns_aliased = [f"{col}_ref" for col in ref_columns]

        try:
            if rule_config.get('sub_category') == 'invalid reference values':
                ref_df = (spark.sql(
                    f"SELECT {', '.join(ref_columns)} FROM {ref_db}.{ref_table} WHERE ds_table_name = '{rule_config['table']}' AND ds_column_name = '{rule_config['columns'][0]}'")).toDF(
                    *ref_columns_aliased)
            else:
                ref_df = (spark.sql(f"SELECT {', '.join(ref_columns)} FROM {ref_db}.{ref_table}")).toDF(
                    *ref_columns_aliased)
        except Exception as e:
            logging.error(
                f"Reference Table {ref_db}.{ref_table} does not exist: {e}, Failed to execute rule_id: {rule_id}")
            return None

        join_columns = dict(zip(rule_config.get('columns', df.columns), ref_columns_aliased))
        join_conditions = []

        # Create case-insensitive dictionaries for column types
        applicable_data_dtypes = {col.lower(): dtype for col, dtype in applicable_data.dtypes}
        ref_df_dtypes = {col.lower(): dtype for col, dtype in ref_df.dtypes}
        for key, value in join_columns.items():
            try:
                # Use lower() to make the lookup case-insensitive
                if applicable_data_dtypes.get(key.lower()) == 'string' and ref_df_dtypes.get(value.lower()) == 'string':
                    join_conditions.append(upper(trim(applicable_data[key])) == upper(trim(ref_df[value])))
                else:
                    join_conditions.append(trim(applicable_data[key]) == trim(ref_df[value]))
            except Exception as e:
                logging.error(f"Error while preparing the join condition for reference check: {e}")

        logging.info(f"join_condition: {join_conditions}")
        df_no_match = applicable_data.join(ref_df, on=join_conditions, how='left_anti') \
            .withColumn("new_status", lit(1)) \
            .withColumn("New_Violation_Details",
                        lit(f"Consistency check failed for columns: {', '.join(rule_config.get('columns', df.columns))}"))

        df_with_match = applicable_data.join(ref_df, on=join_conditions, how='left_semi') \
            .withColumn("new_status", lit(0)) \
            .withColumn("New_Violation_Details", lit(""))
        df = df_no_match.unionByName(df_with_match)

        # Step 4: Update status and violation details
        df = update_status(df)
        df = update_violation_details(df)

        # Drop the aliased reference columns
        df = df.drop(*ref_columns_aliased)

    df = df.unionByName(non_applicable_data)
    return df

def finalize_selection(df, null_flagged_records, rule_config, rule_id, rule_type, system, sub_system, domain):
    """Finalize selection and union null flagged records."""
    df = select_columns(df, rule_config, rule_id, rule_type, system, sub_system, domain)
    if null_flagged_records is not None:
        null_flagged_records = select_columns(null_flagged_records, rule_config, rule_id, rule_type, system, sub_system, domain)
        df = df.unionByName(null_flagged_records)
    return df


def process_rule(rule_id, rule_type, rule_config_json, threshold_values, system, sub_system, domain, df):
    """
    Main function to process a rule by applying various data quality checks and transformations.
    """
    try:
        framework_config.pre_agg_filter = False
        null_flagged_records = None

        logging.info(f"Processing rule: {rule_id}")

        rule_config, parameters = parse_configurations(rule_config_json, threshold_values)
        columns_used_for_calculation = get_columns_used_for_calculation(rule_config, df)

        # Add Key Logs and Composite Attributes
        df = add_key_logs_and_composite_attributes(df, rule_config, columns_used_for_calculation)

        # Perform Null Check
        df, null_flagged_records = perform_null_check(df, columns_used_for_calculation)
        # Validate Emirates ID
        if rule_config.get('sub_category') == "EmiratesID Format Mismatch":
            df = validate_emirates_id_logic(df, rule_config, columns_used_for_calculation)

        # Apply Filter Conditions
        if 'filter_conditions' in rule_config:
            df = apply_pre_aggregation_filter(df, rule_config, parameters)
            framework_config.pre_agg_filter = True

        # Apply Join Conditions
        if 'join_conditions' in rule_config:
            df = apply_join_conditions(df, rule_config)

        # Apply Distinct Operation
        if rule_config.get('distinct', False):
            df = apply_distinct_operation(df, rule_config)

        # Apply Aggregations
        if 'aggregations' in rule_config:
            df = apply_aggregations(df, rule_config, parameters, null_flagged_records)

        # Apply Having Conditions
        if 'having' in rule_config:
            df, null_flagged_records = apply_having_conditions(df, rule_config, parameters, null_flagged_records)

            # Apply Reference Conditions
        if 'reference_conditions' in rule_config:
            df = apply_reference_conditions(df, rule_config, parameters)

        # Final Selection and Union with Null Flagged Records
        df = finalize_selection(df, null_flagged_records, rule_config, rule_id, rule_type, system, sub_system, domain)

        return df

    except Exception as e:
        logging.error(f"Error processing rule: {e}")
        raise


def process_rules_for_table(prepared_rules: List[PreparedRule], original_df: DataFrame) -> Tuple[
    DataFrame, Dict[str, List[str]]]:
    """
    Process all prepared rules for a table.

    Args:
        prepared_rules (List[PreparedRule]): List of prepared rules for the table.
        df (DataFrame): The persisted DataFrame containing the table data.

    Returns:
        Tuple[DataFrame, Dict[str, List[str]]]:
            - DataFrame: The union of all rule results.
            - Dict: A dictionary containing lists of passed and failed rule IDs.
    """
    results = []
    passed_rules = []
    failed_rules = []

    for rule in prepared_rules:
        try:
            # Create a fresh copy of the DataFrame for each rule
            df_copy = original_df.alias("fresh_copy")

            # Call the existing process_rule function
            result = process_rule(
                rule.rule_id,
                rule.rule_type,
                rule.json_config,
                rule.threshold_string,
                rule.system,
                rule.sub_system,
                rule.domain,
                df_copy
            )

            if result is not None:
                # Add rule metadata to the result
                results.append(format_result(result, rule.rule_name, rule.rule_description, rule.json_config))
                passed_rules.append(rule.rule_id)
            else:
                failed_rules.append(rule.rule_id)
                logging.warning(f"Rule {rule.rule_id} returned None result.")

        except Exception as e:
            failed_rules.append(rule.rule_id)
            logging.error(f"Failed to process rule {rule.rule_id}: {str(e)}")

    # Combine all results using union
    if results:
        final_result = reduce(DataFrame.unionByName, results)
    else:
        final_result = None

    rule_summary = {
        "passed_rules": passed_rules,
        "failed_rules": failed_rules
    }

    return final_result, rule_summary

def get_distinct_tables(rule_parameters: DataFrame) -> DataFrame:
    """Get distinct tables from rule_parameters."""
    return rule_parameters.select(
        "database_name", "table_name", "system", "sub_system", "domain", "composite_columns"
    ).dropDuplicates(["table_name"])


def handle_empty_table(
    table_name, load_date, system, sub_system, domain, composite_columns,
    lakehouse_layer, table_summaries
):
    """Handle logic for empty tables."""
    summary_df = generate_empty_table_summary(table_name, load_date, system, sub_system, domain)
    summary_df = summary_df.withColumn("lakehouse_layer", lit(lakehouse_layer))
    logging.info("Started loading summary into summary_log table")
    create_table_if_not_exists("rule_engine_summary_logs_orig", summary_df)
    summary_df.write.format("delta").partitionBy("system", "sub_system", "batch_id", "year_id").mode("append").saveAsTable("rule_engine_summary_logs_orig")

    dq_detail_logs = generate_empty_table_detailed_log(table_name, load_date, system, sub_system, domain, composite_columns)
    dq_detail_logs = dq_detail_logs.withColumn("lakehouse_layer", lit(lakehouse_layer))
    logging.info(f"Started loading detailed logs for {table_name}")
    create_table_if_not_exists("dq_detailed_logs_orig", dq_detail_logs)
    dq_detail_logs.write.format("delta").partitionBy("system", "sub_system", "batch_id", "year_id").mode("append").saveAsTable("dq_detailed_logs_orig")

    logging.warning(f"Table {table_name} is empty.")
    table_summaries[table_name] = {
        "passed_rules": ["DQ_000"],
        "failed_rules": []
    }

ef process_single_table(
    table_row, rules, rule_parameters, load_date, year_id,
    load_only_anomalies_in_detailed_logs, lakehouse_layer,
    sub_source, table_summaries
):
    """Process rules for a single table."""
    database_name = table_row['database_name']
    table_name = table_row['table_name']
    system = table_row['system']
    sub_system = table_row['sub_system']
    domain = table_row['domain']
    composite_columns = table_row['composite_columns']

    logging.info(f"Processing rules for table: {table_name}")

    try:
        prepared_rules = prepare_rules_for_table(rules, rule_parameters, table_name)
        df = read_and_persist_table(database_name, table_name, load_date, year_id, sub_source)

        if df is None or df.count() == 0:
            handle_empty_table(table_name, load_date, system, sub_system, domain, composite_columns, lakehouse_layer, table_summaries)
        else:
            process_non_empty_table(
                table_name, df, prepared_rules, load_only_anomalies_in_detailed_logs,
                lakehouse_layer, system, sub_system, domain, table_summaries
            )

    except Exception as e:
        logging.error(f"Error processing table {table_name}: {str(e)}", exc_info=True)
        table_summaries[table_name] = {
            "passed_rules": [],
            "failed_rules": [rule.rule_id for rule in prepared_rules] if 'prepared_rules' in locals() else []
        }

def process_non_empty_table(
    table_name, df, prepared_rules, load_only_anomalies_in_detailed_logs,
    lakehouse_layer, system, sub_system, domain, table_summaries
):
    """Process logic for non-empty tables."""
    results, rule_summary = process_rules_for_table(prepared_rules, df)

    if results:
        try:
            results = results.persist(StorageLevel.MEMORY_AND_DISK)
            summary_df = generate_summary_df(results).dropDuplicates(["unique_key"])
            summary_df = summary_df.withColumn("lakehouse_layer", lit(lakehouse_layer))

            logging.info("Started loading summary into summary_log table")
            create_table_if_not_exists("rule_engine_summary_logs_orig", summary_df)
            summary_df.write.format("delta").partitionBy("system", "sub_system", "batch_id", "year_id").mode("append").saveAsTable("rule_engine_summary_logs_orig")

            final_result_to_be_saved = results.filter(col("rule_violation_flag") != 0) if load_only_anomalies_in_detailed_logs.lower() == "true" else results
            final_result_to_be_saved = final_result_to_be_saved.persist()

            number_of_records_having_anomalies = final_result_to_be_saved.count()
            logging.info(f"Total Number of anomalies for {table_name}: {number_of_records_having_anomalies}")

            df.unpersist()
            results.unpersist()

            if number_of_records_having_anomalies != 0 or load_only_anomalies_in_detailed_logs.lower() != "true":
                logging.info(f"Started loading detailed logs for {table_name}")
                dq_detail_logs = generate_dq_detailed_logs(final_result_to_be_saved)
                dq_detail_logs = dq_detail_logs.withColumn("lakehouse_layer", lit(lakehouse_layer))
                create_table_if_not_exists("dq_detailed_logs_orig", dq_detail_logs)
                dq_detail_logs.write.format("delta").partitionBy("system", "sub_system", "batch_id", "year_id").mode("append").saveAsTable("dq_detailed_logs_orig")

            table_summaries[table_name] = {
                "passed_rules": rule_summary["passed_rules"],
                "failed_rules": rule_summary["failed_rules"]
            }

            if final_result_to_be_saved.is_cached:
                final_result_to_be_saved.unpersist()

        except Exception as e:
            logging.error(f"Error processing results for table {table_name}: {str(e)}", exc_info=True)
            table_summaries[table_name] = {
                "passed_rules": [],
                "failed_rules": [rule.rule_id for rule in prepared_rules]
            }
    else:
        logging.warning(f"No results generated for table {table_name}")
        table_summaries[table_name] = {
            "passed_rules": [],
            "failed_rules": [rule.rule_id for rule in prepared_rules]
        }


def process_rules_by_table(rules: DataFrame, rule_parameters: DataFrame, load_date: int, year_id: int,
                           load_only_anomalies_in_detailed_logs: str, lakehouse_layer: str, sub_source: str) -> Dict[
    str, Dict[str, List[str]]]:
    """
    Execute data quality rules for each table, applying checks and transformations.
    """
    table_summaries = {}

    try:
        distinct_tables = get_distinct_tables(rule_parameters)

        for table_row in distinct_tables.collect():
            process_single_table(
                table_row, rules, rule_parameters, load_date, year_id,
                load_only_anomalies_in_detailed_logs, lakehouse_layer,
                sub_source, table_summaries
            )

    except Exception as e:
        logging.error(f"Error in process_rules_by_table: {str(e)}", exc_info=True)

    return table_summaries


def format_result(result, rule_name, rule_description, json_config):
    common_columns = ["object", "attribute_name", "dimension", "rule_id", "rule_type"]

    # Combine common and additional columns for grouping and SHA1 key generation
    key_columns = common_columns + framework_config.framework_config['domain_specific_columns']

    return result.select(
        lit(rule_name).alias("rule_name"),
        lit(rule_description).alias("rule_description"),
        col("category").alias("category"),
        col("severity").alias("rule_severity"),
        col("critical_attribute").alias("critical_attribute"),
        col("model_integrity").alias("model_integrity"),
        lit(json_config).alias("rule_json"),
        *key_columns,
        col("Key_Logs").alias("source_table_attributes"),
        col("source_table_composite_attributes_name").alias("source_table_composite_attributes_name"),
        col("source_table_composite_attributes_values").alias("source_table_composite_attributes_values"),
        col("value_of_attributes_used_for_calculation").alias("value_of_attributes_used_for_calculation"),
        col("rule_violation_flag").alias("rule_violation_flag"),
        col("Violation_Details").alias("rule_violation_details"),
        lit(framework_config.rule_run_date).alias("rule_run_date"),
        "year_id",
        "batch_id"
    )


def format_summary(summary):
    common_columns = ["object", "attribute_name", "dimension", "rule_id", "rule_type"]

    # Combine common and additional columns for grouping and SHA1 key generation
    key_columns = common_columns + framework_config.framework_config['domain_specific_columns']
    return summary.select(
        "unique_key",
        *key_columns,
        col("rule_name").alias("applied_rule"),
        "rule_description",
        "total_record_count",
        "no_of_complete_records",
        "total_passed_count",
        "no_of_failures",
        "value",
        "year_id",
        "batch_id",
        col("rule_run_date").cast("timestamp").alias("rule_run_date")
    )


def generate_summary_df(df):
    """
    Generates a summary DataFrame from the processed data using a simplified rule violation flag logic.

    Args:
    df (DataFrame): The Spark DataFrame after data quality checks.

    Returns:
    DataFrame: A DataFrame summarizing the results of the checks.
    """
    # Common columns across all projects
    common_columns = ["rule_id", "dimension", "object", "attribute_name", "year_id", "batch_id"]
    # Combine common and additional columns for grouping and SHA1 key generation
    key_columns = common_columns + framework_config.framework_config['domain_specific_columns']
    # Simplified aggregation logic with no_of_complete_records added
    summary_df = df.groupBy(*key_columns).agg(
        count("*").alias("total_record_count"),
        sum(when(col("rule_violation_flag").isin(-1, 1, -2), 1).otherwise(0)).alias("no_of_failures"),
        sum(when(col("rule_violation_flag") == 0, 1).otherwise(0)).alias("total_passed_count"),
        sum(when(col("rule_violation_flag") != -1, 1).otherwise(0)).alias("no_of_complete_records")
    )
    # Add rule_run_date
    summary_df = summary_df.withColumn("rule_run_date", lit(framework_config.rule_run_date))

    # Update key_columns to include rule_run_date
    key_columns = common_columns + ["rule_run_date"] + framework_config.framework_config['domain_specific_columns']

    # Join with unique_rules_df to get rule_type and other details
    unique_rules_df = df.select("rule_id", "rule_description", "rule_name", "rule_type").dropDuplicates()
    summary_df = summary_df.join(unique_rules_df, on="rule_id", how="left")
    # Calculate value (pass rate)
    summary_df = summary_df.withColumn(
        "value",
        when(col("total_record_count") == 0, lit(0))
        .otherwise(col("total_passed_count") / col("total_record_count"))
    )
    # Generate unique key and select final columns
    summary = summary_df.selectExpr(
        f"sha2(concat_ws('-', {', '.join(key_columns)}), 256) as unique_key",
        *key_columns,
        "total_record_count",
        "no_of_complete_records",
        "total_passed_count",
        "no_of_failures",
        "value",
        "rule_description",
        "rule_name",
        "rule_type"
    )
    final_summary = format_summary(summary)
    return final_summary


def generate_empty_table_summary(table_name, load_date, system, sub_system, domain):
    """
    Generate a summary DataFrame for an empty table.

    Args:
        table_name (str): Name of the empty table.
        load_date (int): The load date for data filtering.
        system (str): The system name.
        sub_system (str): The sub-system name.
        domain (str): The domain name.

    Returns:
        DataFrame: A summary DataFrame for the empty table (after formatting).
    """

    # Define schema explicitly
    schema = StructType([
        StructField("object", StringType(), True),
        StructField("attribute_name", StringType(), True),
        StructField("dimension", StringType(), True),
        StructField("rule_id", StringType(), True),
        StructField("rule_type", StringType(), True),
        StructField("rule_name", StringType(), True),
        StructField("rule_description", StringType(), True),
        StructField("total_record_count", LongType(), True),
        StructField("no_of_complete_records", LongType(), True),
        StructField("total_passed_count", LongType(), True),
        StructField("no_of_failures", LongType(), True),
        StructField("value", DoubleType(), True),
        StructField("year_id", IntegerType(), True),  # IntegerType, but nullable
        StructField("batch_id", IntegerType(), True),
        StructField("rule_run_date", StringType(), True),
        StructField("system", StringType(), True),
        StructField("sub_system", StringType(), True),
        StructField("domain", StringType(), True)
    ])

    # Create a single-row DataFrame with all required columns
    summary_df = spark.createDataFrame([(
        table_name,  # object
        "All Columns",  # attribute_name
        "completeness",  # dimension
        "DQ_000",  # rule_id
        "Data Quality",  # rule_type
        "Table Empty Check",  # rule_name
        "Table has no data",  # rule_description
        0,  # total_record_count
        0,
        0,  # total_passed_count
        1,  # no_of_failures
        0.0,  # value
        None,  # year_id (null for empty/unknown year)
        int(load_date),  # batch_id
        framework_config.rule_run_date,  # rule_run_date
        system,  # system
        sub_system,  # sub_system
        domain  # domain
    )], schema=schema)

    # Common columns across all projects
    common_columns = ["rule_id", "dimension", "object", "attribute_name", "year_id", "batch_id", "rule_run_date"]
    # Combine common and additional columns for grouping and SHA1 key generation
    key_columns = common_columns + framework_config.framework_config['domain_specific_columns']

    summary = summary_df.selectExpr(
        f"sha2(concat_ws('-', {', '.join(key_columns)}), 256) as unique_key",
        "*"
    )
    final_summary = format_summary(summary)
    return final_summary


def generate_dq_detailed_logs(df):
    """
       Generate detailed Data Quality (DQ) logs by extracting and transforming attributes from the provided DataFrame.

       Args:
           df (DataFrame): The DataFrame containing rule execution results and attributes to be logged.

       Returns:
           DataFrame: The DataFrame with detailed DQ logs, including extracted keys and values, and additional metadata.
    """

    # Common columns across all projects
    common_columns = ["rule_id", "dimension", "object", "attribute_name", "year_id", "batch_id", "rule_run_date"]

    # Combine common and additional columns for grouping and SHA1 key generation
    key_columns = common_columns + framework_config.framework_config['domain_specific_columns']

    df = df.filter(col("rule_violation_flag").isin(-1, -2, 1))

    # Split the composite attributes into keys and initialize the key-value pairs in a single expression

    df = df.select(
        *df.columns,
        split(col("source_table_composite_attributes_name"), "-").alias("keys_array"),
        *[lit(None).alias(f"key{i}") for i in range(1, 5)],
        *[lit(None).alias(f"value{i}") for i in range(1, 5)]
    )

    df = extract_keys_values(df)
    logging.info(f"Column used for unique_key generation in summary: {key_columns}")
    return df.selectExpr(
        f"sha2(concat_ws('-', {', '.join(key_columns)}), 256) as unique_key",
        *key_columns,
        "rule_type",
        "rule_name as applied_rule",
        "rule_description",
        "rule_json",
        "category",
        "critical_attribute",
        "model_integrity",
        "source_table_attributes",
        "rule_violation_flag as result",
        "rule_violation_details",
        "value_of_attributes_used_for_calculation as value",
        "source_table_composite_attributes_name as key_columns",
        "source_table_composite_attributes_values as values_of_key_columns",
        "key1",
        "value1",
        "key2",
        "value2",
        "key3",
        "value3",
        "key4",
        "value4"
    ).withColumn("rule_run_date", col("rule_run_date").cast("timestamp"))


def generate_empty_table_detailed_log(table_name, load_date, system, sub_system, domain, composite_columns):
    """
    Generate a detailed log entry for an empty table.

    Args:
        table_name (str): Name of the empty table.
        load_date (int): The load date for data filtering.
        system (str): The system name.
        sub_system (str): The sub-system name.
        domain (str): The domain name.
        composite_columns (str): Comma-separated list of composite columns.

    Returns:
        DataFrame: A DataFrame representing a single row for dq_detailed_logs.
    """
    logging.info(f"composit column:{composite_columns}")
    # Common columns across all projects
    common_columns = ["rule_id", "dimension", "object", "attribute_name", "year_id", "batch_id", "rule_run_date"]
    # Combine common and additional columns for grouping and SHA1 key generation
    key_columns = common_columns + framework_config.framework_config['domain_specific_columns']

    # Convert comma-separated composite columns to hyphen-separated
    composite_columns_hyphen = composite_columns.replace(',', '-') if composite_columns else ''

    # Create a list of composite columns
    composite_column_list = composite_columns.split(',') if composite_columns else []
    composite_column_list = [col.strip() for col in composite_column_list]

    # Prepare key1, key2, key3, key4 with composite column names and empty values for value1, value2, etc.
    key_values = []
    for i in range(4):
        if i < len(composite_column_list):
            key_values.extend([composite_column_list[i], ''])  # key filled, value empty
        else:
            key_values.extend(['', ''])  # both key and value empty

    schema = StructType([
        StructField("object", StringType(), True),
        StructField("attribute_name", StringType(), True),
        StructField("dimension", StringType(), True),
        StructField("rule_id", StringType(), True),
        StructField("rule_type", StringType(), True),
        StructField("applied_rule", StringType(), True),
        StructField("rule_description", StringType(), True),
        StructField("rule_json", StringType(), True),
        StructField("category", StringType(), True),
        StructField("critical_attribute", StringType(), True),
        StructField("model_integrity", StringType(), True),
        StructField("source_table_attributes", StringType(), True),
        StructField("result", IntegerType(), True),
        StructField("rule_violation_details", StringType(), True),
        StructField("value", StringType(), True),
        StructField("key_columns", StringType(), True),
        StructField("values_of_key_columns", StringType(), True),
        StructField("year_id", IntegerType(), True),
        StructField("batch_id", IntegerType(), True),
        StructField("rule_run_date", StringType(), True),
        StructField("system", StringType(), True),
        StructField("sub_system", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("key1", StringType(), True),
        StructField("value1", StringType(), True),
        StructField("key2", StringType(), True),
        StructField("value2", StringType(), True),
        StructField("key3", StringType(), True),
        StructField("value3", StringType(), True),
        StructField("key4", StringType(), True),
        StructField("value4", StringType(), True)
    ])

    # Create the DataFrame
    df = spark.createDataFrame([(
        table_name,  # object
        "All Columns",  # attribute_name
        "completeness",  # dimension
        "DQ_000",  # rule_id
        "Data Quality",  # rule_type
        "Table Empty Check",  # applied_rule (rule_name)
        "Table has no data",  # rule_description
        json.dumps({"message": "Table is empty"}),  # rule_json
        "completeness",  # category
        "",  # critical_attribute
        "",  # model_integrity
        "{}",  # source_table_attributes (empty JSON object)
        1,  # result (rule_violation_flag)
        "Table is empty",  # rule_violation_details
        "",  # value
        composite_columns_hyphen,  # key_columns (source_table_composite_attributes_name)
        "",  # values_of_key_columns (source_table_composite_attributes_values)
        None,  # year_id
        int(load_date),  # batch_id
        framework_config.rule_run_date,  # rule_run_date
        system,  # system
        sub_system,  # sub_system
        domain,  # domain
        *key_values
    )], schema=schema)

    # Generate the unique key
    df = df.selectExpr(
        f"sha2(concat_ws('-', {', '.join(key_columns)}), 256) as unique_key",
        "*"
    )

    # Ensure all required columns are present and in the correct order
    return df.select(
        "unique_key",
        *key_columns,
        "rule_type",
        "applied_rule",
        "rule_description",
        "rule_json",
        "category",
        "critical_attribute",
        "model_integrity",
        "source_table_attributes",
        "result",
        "rule_violation_details",
        "value",
        "key_columns",
        "values_of_key_columns",
        "key1", "value1",
        "key2", "value2",
        "key3", "value3",
        "key4", "value4"
    ).withColumn("rule_run_date", col("rule_run_date").cast("timestamp"))


def print_table_summaries(table_summaries):
    """
    Print a summary of the rule execution results for each table.

    Args:
        table_summaries (Dict[str, Dict[str, List[str]]]): The summaries of rule execution for each table.
    """
    logging.info("\n=== Rule Execution Summary ===")
    rule_execution_summary = {}
    total_passed = 0
    total_failed = 0

    for table_name, summary in table_summaries.items():
        passed_count = len(summary['passed_rules'])
        failed_count = len(summary['failed_rules'])

        logging.info(f"\nTable: {table_name}")
        logging.info(f"Passed Rules: {passed_count}")
        logging.info(f"Failed Rules: {failed_count}")

        if failed_count > 0:
            logging.info("Failed Rule IDs:")
            for rule_id in summary['failed_rules']:
                logging.info(f"  - {rule_id}")

        logging.info("-" * 30)

        total_passed += passed_count
        total_failed += failed_count
    rule_execution_summary = {"total_passed": total_passed, "total_failed": total_failed}
    logging.info(f"\nTotal Passed Rules: {total_passed}")
    logging.info(f"Total Failed Rules: {total_failed}")
    logging.info("==========================")
    return rule_execution_summary

def log_execution(
    target_table,
    notebook_name,
    pipeline_name,
    layer,
    system,
    sub_system,
    status,
    details,
    error_message=None,
    execution_end_time=None
):
    # Prepare the log record
    log_data = [{
        "Notebook_Name": notebook_name,
        "Pipeline_Name": pipeline_name,
        "Layer": layer,
        "System": system,
        "Sub_System": sub_system,
        "Run_Date": framework_config.rule_run_date,  # Only the date part
        "Execution_Start_Time": datetime.now(),
        "Execution_End_Time": execution_end_time,
        "Status": status,
        "Details": details,
        "Error_Message": error_message
    }]

    # Define schema explicitly
    schema = StructType([
        StructField("Notebook_Name", StringType(), True),
        StructField("Pipeline_Name", StringType(), True),
        StructField("Layer", StringType(), True),
        StructField("System", StringType(), True),
        StructField("Sub_System", StringType(), True),
        StructField("Run_Date", StringType(), True),
        StructField("Execution_Start_Time", TimestampType(), True),
        StructField("Execution_End_Time", TimestampType(), True),
        StructField("Status", StringType(), True),
        StructField("Details", StringType(), True),
        StructField("Error_Message", StringType(), True),
    ])

    # Create a DataFrame with the defined schema
    source_df = spark.createDataFrame(log_data, schema)

    # Define the primary keys for the MERGE operation
    keys = ["Notebook_Name", "Run_Date", "System", "Sub_System"]

    # Call the merge_config_tables function
    merge_logs_tables(target_table, source_df, keys)
    logging.info("Execution details logged (merge operation).")

def apply_distinct_operation(df, rule_config):
    """Apply distinct operation based on the configuration."""
    if 'distinct_columns' in rule_config and rule_config['distinct_columns']:
        return df.dropDuplicates(rule_config['distinct_columns'])
    return df.distinct()

def configure_spark(spark_session):
    """Configures the Spark session for the job."""
    # Example of dyncamic configurations. Replace with actual configuration keys and values.
    spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark_session.conf.set("spark.sql.adaptive.enabled", "true")
    spark_session.conf.set("spark.sql.parser.escapedStringLiterals", "true")
    #spark_session.conf.set("spark.sql.shuffle.partitions", 200)

def get_runtime_config(spark, type='all'):
    """Retrieves runtime configuration from Spark context."""
    # Fetch configurations, using defaults if not provided
    database = spark.sparkContext.getConf().get("spark.ruleengine.database", "default")
    rule_type = spark.sparkContext.getConf().get("spark.ruleengine.rule.type", "all")
    rules_to_be_executed = spark.sparkContext.getConf().get("spark.ruleengine.rule.ids", "all")
    sub_verticals_to_be_executed = spark.sparkContext.getConf().get("spark.ruleengine.subverticals", "all")
    verticals_to_be_executed = spark.sparkContext.getConf().get("spark.ruleengine.verticals", "all")
    entities_to_be_executed = spark.sparkContext.getConf().get("spark.ruleengine.entities", "all")
    number_of_output_files = int(spark.sparkContext.getConf().get("spark.ruleengine.output.files", "100"))
    postgres_ip = spark.sparkContext.getConf().get("spark.postgres.ip")
    postgres_port = spark.sparkContext.getConf().get("spark.postgres.port")
    postgres_user = spark.sparkContext.getConf().get("spark.postgres.user")
    postgres_password = spark.sparkContext.getConf().get("spark.postgres.password")

    # Add more configurations as needed

    if type == 'ruleengine':
      return database, rule_type, rules_to_be_executed, sub_verticals_to_be_executed, verticals_to_be_executed, entities_to_be_executed, number_of_output_files
    elif type == 'postgres':
      return postgres_ip, postgres_port, postgres_user, postgres_password
    else:
      return database, rule_type, rules_to_be_executed, sub_verticals_to_be_executed, verticals_to_be_executed, entities_to_be_executed, number_of_output_files, postgres_ip, postgres_port, postgres_user, postgres_password

def get_list_of_special_functions():
    return ["count_distinct","countdistinct","approx_count_distinct","collect_list","collect_set","first","last"]

def load_rule_parameters(spark, rule_id):
    """
    Load parameters for a given rule ID from the rule_parameters table.
    """
    try:
        postgres_ip, postgres_port, postgres_user, postgres_password = get_runtime_config(spark, 'postgres')

        rule_params_df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{postgres_ip}:{postgres_port}/postgres") \
            .option("dbtable", "ruleengine.rule_parameters") \
            .option("user", postgres_user) \
            .option("password", postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        rule_params_df = rule_params_df.filter((col("is_active") == 1) & (col("ruleid") == rule_id))

        return rule_params_df
    except Exception as e:
        logging.error(f"Error processing rule: {e}")
        raise

def read_from_jdbc(spark, url, dbtable, user, password, driver):
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", dbtable) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver) \
            .load()
        return df
    except Exception as e:
        logging.error(f"Error reading from JDBC source {dbtable}: {e}")
        raise

def load_rule_engine_configurations(spark,rule_type):
    """Loads rule engine and threshold configurations from external sources."""
    # Placeholder for actual loading logic
    try:
        postgres_ip, postgres_port, postgres_user, postgres_password = get_runtime_config(spark, 'postgres')

        url = f"jdbc:postgresql://{postgres_ip}:{postgres_port}/postgres"
        driver = "org.postgresql.Driver"

        rule_engine = read_from_jdbc(spark, url, "ruleengine.rule_engine", postgres_user, postgres_password, driver)
        rule_engine = rule_engine.filter(col("ISACTIVE") == 1)

        if rule_type != 'all':
            rule_engine = rule_engine.filter(col("rule_type") == rule_type)

        rule_threshold = read_from_jdbc(spark, url, "ruleengine.rule_threshold", postgres_user, postgres_password,
                                        driver)
        rule_threshold = rule_threshold.filter(col("ISACTIVE") == 1)

        if rule_type == 'all' or rule_type == 'Data Quality':
            rule_params_df = read_from_jdbc(spark, url, "ruleengine.rule_parameters", postgres_user, postgres_password,
                                            driver)
        else:
            rule_params_df = None

        return rule_engine, rule_threshold, rule_params_df
    except Exception as e:
        logging.error(f"Error processing rule: {e}")
        raise


def initialize_spark_session():
    return SparkSession.builder.enableHiveSupport() \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.session.state.builder", "org.apache.spark.sql.hive.UQueryHiveACLSessionStateBuilder") \
        .config("spark.security.credentials.hive.enabled", "true") \
        .config("spark.sql.catalog.class", "org.apache.spark.sql.hive.UQueryHiveACLExternalCatalog") \
        .config("spark.sql.extensions", "org.apache.spark.sql.DliSparkExtension") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.parser.escapedStringLiterals", "true") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.2") \
        .config("hive.exec.dynamic.partition","true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.jsonGenerator.ignoreNullFields", "false") \
        .appName("rule-engine").getOrCreate()
def load_configurations(spark, rule_type, rules_to_be_executed, sub_verticals_to_be_executed, verticals_to_be_executed):
    if rule_type not in ['Data Quality', 'AUDIT']:
        logging.info("Invalid rule type. Please refer to the manual.")
        return None, None, None

    logging.info(f"Started Loading Rule Engine Config for rules: {rules_to_be_executed}, vertical: {verticals_to_be_executed}")

    rule_engine, rule_threshold, rule_parameters = load_rule_engine_configurations(spark, rule_type)
    rule_engine = rule_engine.filter(col("rule_type") == rule_type)

    if rules_to_be_executed.lower() != 'all':
        list_of_rules_to_be_executed = rules_to_be_executed.split(",")
        rule_engine = rule_engine.filter(col("ruleid").isin(list_of_rules_to_be_executed))
        rule_threshold = rule_threshold.filter(col("ruleid").isin(list_of_rules_to_be_executed))
        if rule_parameters is not None:
            rule_parameters = rule_parameters.filter(col("ruleid").isin(list_of_rules_to_be_executed))

    if sub_verticals_to_be_executed.lower() != 'all':
        list_of_sub_verticals_to_be_executed = sub_verticals_to_be_executed.split(",")
        if rule_parameters is not None:
            rule_parameters = rule_parameters.filter(col("sub_vertical").isin(list_of_sub_verticals_to_be_executed))
        else:
            rule_engine = rule_engine.filter(col("subvertical").isin(list_of_sub_verticals_to_be_executed))
            rule_threshold = rule_threshold.filter(col("subvertical").isin(list_of_sub_verticals_to_be_executed))

    if verticals_to_be_executed.lower() != 'all':
        list_of_verticals_to_be_executed = verticals_to_be_executed.split(",")
        if rule_parameters is not None:
            rule_parameters = rule_parameters.filter(col("vertical").isin(list_of_verticals_to_be_executed))
        else:
            rule_engine = rule_engine.filter(col("vertical").isin(list_of_verticals_to_be_executed))
            rule_threshold = rule_threshold.filter(col("vertical").isin(list_of_verticals_to_be_executed))

    return rule_engine, rule_threshold, rule_parameters

def prepare_rules(rule_engine, rule_threshold):
    return join_dataframes(rule_engine, rule_threshold, "left_outer", join_columns='ruleid,entity,vertical,subvertical').select(
        "ruleid", "rulename", "ruledescription", "vertical", "subvertical", "rule", "entity", "thresholdvalue", "is_template", "rule_type")

def generate_template_config(rule, param_row, column, critical_attribute, model_integrity):
    column = column.split(',')
    composite_columns = param_row['composite_columns'].split(',')
    reference_database = param_row['reference_database']
    reference_table_name = param_row['reference_table_name']
    reference_columns = param_row['reference_columns'].split(",") if param_row['reference_columns'] else None
    params = {
        'database': param_row['database_name'],
        'table': param_row['table_name'],
        'columns': column,
        'mandatory_columns': param_row['mandatory_columns'] if param_row['mandatory_columns'] == 'false' else column,
        'composite_columns': composite_columns,
        'only_passed_records': param_row['only_passed_records'],
        'critical_attribute': critical_attribute,
        'model_integrity': model_integrity
    }

    if reference_database is not None:
        params.update({
            'reference_database': reference_database,
            'reference_table_name': reference_table_name,
            'reference_columns': reference_columns
        })

    return generate_json_config(rule, params)


if __name__ == "__main__":
    try:
        spark = initialize_spark_session()
        configure_spark(spark)

        setup_logging()
        logging.info(f"Initializing the DQ Framework")



        database, rule_type, rules_to_be_executed, sub_verticals_to_be_executed, verticals_to_be_executed, entities_to_be_executed, number_of_output_files = get_runtime_config(spark,'ruleengine')

        logging.info(f"Started rule engine for {rule_type}")

        load_framework_config(spark)
        refresh_config_on_lake(spark)


        rule_engine, rule_threshold, rule_parameters = load_configurations(spark, rule_type, rules_to_be_executed,
                                                                           sub_verticals_to_be_executed,
                                                                           verticals_to_be_executed)
        if rule_engine is None:
            logging.info(
                "No Config found for specified rule,entity,vertical,subvertical, kindly check the config in Postgres DB")
            exit()

        rules = prepare_rules(rule_engine, rule_threshold)

        if rules.count() == 0:
            logging.info("No Config found for specified rule,entity,vertical,subvertical, kindly check the config in Postgres DB")
            exit()


        table_summaries = process_rules_by_table(rules, rule_parameters, load_date, year_id, load_only_anomalies_in_detailed_logs, lakehouse_layer,sub_source)

        if not table_summaries:
            logging.info("No Rules were executed. Ending the rule engine execution.")
            exit()

        rule_execution_summary = print_table_summaries(table_summaries)
        total_passed = rule_execution_summary.get("total_passed", 0)
        total_failed = rule_execution_summary.get("total_failed", 0)
        details = f"Execution completed: total_passed_rules = {total_passed}, total_failed_rules = {total_failed}"


    except Exception as e:
        logging.error("An unexpected error occurred during the execution of the rule engine.", exc_info=True)
    finally:
        spark.stop()