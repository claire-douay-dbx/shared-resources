"""
Databricks schema creation script for date_dimensions table.
This script defines the schema and creates the table in Databricks.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DateType,
)

# Initialize Spark Session (already available in Databricks as 'spark')
# spark = SparkSession.builder.appName("DateDimensionsSchema").getOrCreate()

# Define the schema for date_dimensions
date_dimensions_schema = StructType([
    StructField("date_key", LongType(), False),
    StructField("full_date", DateType(), False),
    StructField("calendar_year", LongType(), False),
    StructField("calendar_quarter", LongType(), False),
    StructField("calendar_quarter_name", StringType(), False),
    StructField("month_number", LongType(), False),
    StructField("month_name", StringType(), False),
    StructField("month_short", StringType(), False),
    StructField("year_month", StringType(), False),
    StructField("month_year_label", StringType(), False),
    StructField("week_of_year", LongType(), False),
    StructField("year_week", StringType(), False),
    StructField("day_of_month", LongType(), False),
    StructField("day_of_week_number", LongType(), False),
    StructField("day_of_week_name", StringType(), False),
    StructField("day_of_week_short", StringType(), False),
    StructField("day_of_year", LongType(), False),
    StructField("fiscal_year", LongType(), False),
    StructField("fiscal_year_label", StringType(), False),
    StructField("fiscal_quarter", LongType(), False),
    StructField("fiscal_quarter_name", StringType(), False),
    StructField("fiscal_month", LongType(), False),
    StructField("fiscal_period", StringType(), False),
    StructField("retail_season", StringType(), False),
    StructField("promotional_period", StringType(), False),
    StructField("is_weekend", StringType(), False),
    StructField("is_working_day", StringType(), False),
    StructField("is_holiday", StringType(), False),
    StructField("holiday_name", StringType(), True),
    StructField("is_month_start", StringType(), False),
    StructField("is_month_end", StringType(), False),
    StructField("is_quarter_start", StringType(), False),
    StructField("is_quarter_end", StringType(), False),
    StructField("is_year_start", StringType(), False),
    StructField("is_year_end", StringType(), False),
    StructField("is_fiscal_year_start", StringType(), False),
    StructField("is_fiscal_year_end", StringType(), False),
    StructField("month_sort", LongType(), False),
    StructField("day_of_week_sort", LongType(), False),
])


def create_date_dimensions_table(
    spark,
    catalog: str = "asda_arch_day",
    schema: str = "retail",
    table_name: str = "date_dimensions",
    csv_path: str = None,
):
    """
    Create the date_dimensions table in Databricks Unity Catalog.
    
    Args:
        spark: SparkSession object
        catalog: Catalog name in Unity Catalog
        schema: Schema/database name
        table_name: Name of the table to create
        csv_path: Optional path to CSV file to load data from
    """
    
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    # Create catalog and schema if they don't exist
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    
    # Drop table if exists (optional - remove if you want to preserve existing data)
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    
    # Create the table using SQL DDL
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        date_key BIGINT NOT NULL COMMENT 'A unique identifier for each date entry, which can be used to join with other tables for analysis.',
        full_date DATE NOT NULL COMMENT 'Represents the complete date in a standard format, useful for any date-related queries or reports.',
        calendar_year BIGINT NOT NULL COMMENT 'Indicates the year in the Gregorian calendar, which is essential for annual reporting and trend analysis.',
        calendar_quarter BIGINT NOT NULL COMMENT 'Represents the quarter of the year (1 to 4), helping to analyze performance over quarterly periods.',
        calendar_quarter_name STRING NOT NULL COMMENT 'Provides a textual representation of the quarter (e.g., Q1, Q2), making it easier to understand and present data.',
        month_number BIGINT NOT NULL COMMENT 'The numerical representation of the month (1 to 12), useful for sorting and filtering data by month.',
        month_name STRING NOT NULL COMMENT 'The full name of the month (e.g., January, February), which can enhance readability in reports.',
        month_short STRING NOT NULL COMMENT 'A shortened version of the month name (e.g., Jan, Feb), useful for compact displays and summaries.',
        year_month STRING NOT NULL COMMENT 'Combines the year and month into a string format (YYYY-MM), facilitating time-based analysis at the month level.',
        month_year_label STRING NOT NULL COMMENT 'A label combining the month and year, useful for creating clear and informative time-based reports.',
        week_of_year BIGINT NOT NULL COMMENT 'Indicates the week number within the year (1 to 52), which can be useful for weekly performance tracking.',
        year_week STRING NOT NULL COMMENT 'A string representation of the year and week number, useful for reporting and analysis at the weekly level.',
        day_of_month BIGINT NOT NULL COMMENT 'The day of the month (1 to 31), which can be used for detailed day-level analysis.',
        day_of_week_number BIGINT NOT NULL COMMENT 'Represents the day of the week as a number (1 for Sunday to 7 for Saturday), useful for scheduling and analysis.',
        day_of_week_name STRING NOT NULL COMMENT 'The full name of the day of the week (e.g., Monday, Tuesday), enhancing clarity in reports.',
        day_of_week_short STRING NOT NULL COMMENT 'A shortened version of the day name (e.g., Mon, Tue), useful for compact displays.',
        day_of_year BIGINT NOT NULL COMMENT 'Indicates the day number within the year (1 to 365 or 366), useful for specific day-level analysis.',
        fiscal_year BIGINT NOT NULL COMMENT 'Represents the fiscal year, which may differ from the calendar year, important for financial reporting.',
        fiscal_year_label STRING NOT NULL COMMENT 'A label for the fiscal year, aiding in clarity for financial documents and reports.',
        fiscal_quarter BIGINT NOT NULL COMMENT 'Indicates the fiscal quarter (1 to 4), essential for analyzing financial performance over fiscal periods.',
        fiscal_quarter_name STRING NOT NULL COMMENT 'Provides a textual representation of the fiscal quarter, useful for financial reporting.',
        fiscal_month BIGINT NOT NULL COMMENT 'Represents the month within the fiscal year, which may differ from the calendar month, important for financial analysis.',
        fiscal_period STRING NOT NULL COMMENT 'A string that describes the fiscal period, useful for detailed financial reporting.',
        retail_season STRING NOT NULL COMMENT 'Indicates specific retail seasons (e.g., holiday season), which can be critical for sales analysis.',
        promotional_period STRING NOT NULL COMMENT 'Describes periods of promotional activity, useful for analyzing the impact of marketing efforts.',
        is_weekend STRING NOT NULL COMMENT 'A flag indicating whether the date falls on a weekend, useful for scheduling and operational planning.',
        is_working_day STRING NOT NULL COMMENT 'A flag indicating whether the date is a working day, important for workforce management and planning.',
        is_holiday STRING NOT NULL COMMENT 'A flag indicating whether the date is a recognized holiday, which can affect business operations.',
        holiday_name STRING COMMENT 'The name of the holiday, providing context for any operational changes or reporting.',
        is_month_start STRING NOT NULL COMMENT 'A flag indicating if the date is the start of a month, useful for month-end and month-start analyses.',
        is_month_end STRING NOT NULL COMMENT 'A flag indicating if the date is the end of a month, important for month-end reporting.',
        is_quarter_start STRING NOT NULL COMMENT 'A flag indicating if the date is the start of a quarter, useful for quarterly reporting.',
        is_quarter_end STRING NOT NULL COMMENT 'A flag indicating if the date is the end of a quarter, important for quarterly financial analysis.',
        is_year_start STRING NOT NULL COMMENT 'A flag indicating if the date is the start of a year, useful for annual reporting.',
        is_year_end STRING NOT NULL COMMENT 'A flag indicating if the date is the end of a year, important for year-end financial analysis.',
        is_fiscal_year_start STRING NOT NULL COMMENT 'A flag indicating if the date is the start of the fiscal year, crucial for financial planning.',
        is_fiscal_year_end STRING NOT NULL COMMENT 'A flag indicating if the date is the end of the fiscal year, important for year-end financial reporting.',
        month_sort BIGINT NOT NULL COMMENT 'A numerical value used for sorting months in chronological order, facilitating time-based analysis.',
        day_of_week_sort BIGINT NOT NULL COMMENT 'A numerical value used for sorting days of the week, aiding in scheduling and analysis.'
    )
    USING DELTA
    COMMENT 'Date dimension table for retail analytics'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    spark.sql(create_table_sql)
    
    # Add Primary Key constraint
    spark.sql(f"""
        ALTER TABLE {full_table_name} 
        ADD CONSTRAINT date_dimensions_pk PRIMARY KEY (date_key)
    """)
    
    # Add tags to the table (key-value format)
    spark.sql(f"ALTER TABLE {full_table_name} SET TAGS ('asda_articles' = 'silver_articles')")
    
    print(f"Table {full_table_name} created successfully with PK: date_key, tag: asda_articles=silver_articles")
    
    # Load data from CSV if path is provided
    if csv_path:
        df = spark.read.csv(
            csv_path,
            header=True,
            schema=date_dimensions_schema,
            dateFormat="dd-MM-yyyy",
        )
        df.write.mode("overwrite").saveAsTable(full_table_name)
        print(f"Data loaded into {full_table_name} from {csv_path}")
    
    return full_table_name


def load_date_dimensions_from_csv(
    spark,
    csv_path: str,
    catalog: str = "asda_arch_day",
    schema: str = "retail",
    table_name: str = "date_dimensions",
):
    """
    Load date_dimensions data from a CSV file into an existing table.
    
    Args:
        spark: SparkSession object
        csv_path: Path to the CSV file
        catalog: Catalog name in Unity Catalog
        schema: Schema/database name
        table_name: Name of the table
    """
    
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    df = spark.read.csv(
        csv_path,
        header=True,
        schema=date_dimensions_schema,
        dateFormat="dd-MM-yyyy",
    )
    
    df.write.mode("overwrite").saveAsTable(full_table_name)
    print(f"Data loaded into {full_table_name} from {csv_path}")
    
    # Display record count
    count = spark.table(full_table_name).count()
    print(f"Total records: {count}")


# Example usage in Databricks notebook:
# create_date_dimensions_table(spark, catalog="asda_arch_day", schema="retail")
# load_date_dimensions_from_csv(spark, "dbfs:/mnt/data/date_dimensions.csv")
