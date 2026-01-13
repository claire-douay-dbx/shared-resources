"""
Databricks schema creation script for transactions table.
This script defines the schema and creates the table in Databricks.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
    DateType,
)

# Initialize Spark Session (already available in Databricks as 'spark')
# spark = SparkSession.builder.appName("TransactionsSchema").getOrCreate()

# Define the schema for transactions
transactions_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("basket_id", StringType(), False),
    StructField("article_id", StringType(), False),
    StructField("transaction_date", DateType(), False),
    StructField("quantity", LongType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("discount_pct", LongType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("store_id", StringType(), False),
    StructField("store_name", StringType(), False),
    StructField("region", StringType(), False),
    StructField("store_type", StringType(), False),
    StructField("sales_channel", StringType(), False),
    StructField("customer_id", StringType(), True),
])


def create_transactions_table(
    spark,
    catalog: str = "asda_arch_day",
    schema: str = "retail",
    table_name: str = "transactions",
    csv_path: str = None,
):
    """
    Create the transactions table in Databricks Unity Catalog.
    
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
        transaction_id STRING NOT NULL COMMENT 'A unique identifier for each transaction, allowing for tracking and referencing specific sales events.',
        basket_id STRING NOT NULL COMMENT 'Identifies the shopping basket associated with the transaction, which can be useful for analyzing customer purchasing patterns.',
        article_id STRING NOT NULL COMMENT 'Represents the unique identifier for each product sold in the transaction, facilitating inventory and sales analysis.',
        transaction_date DATE NOT NULL COMMENT 'The date when the transaction occurred, essential for time-based sales analysis and trend identification.',
        quantity BIGINT NOT NULL COMMENT 'Indicates the number of units sold in the transaction, providing insight into product demand and sales volume.',
        unit_price DOUBLE NOT NULL COMMENT 'The price per unit of the product at the time of the transaction, useful for calculating revenue and pricing strategies.',
        discount_pct BIGINT NOT NULL COMMENT 'The percentage discount applied to the transaction, which helps in evaluating promotional effectiveness and pricing adjustments.',
        total_amount DOUBLE NOT NULL COMMENT 'The total monetary value of the transaction after applying any discounts, crucial for revenue analysis.',
        store_id STRING NOT NULL COMMENT 'A unique identifier for the store where the transaction took place, aiding in store performance evaluation.',
        store_name STRING NOT NULL COMMENT 'The name of the store involved in the transaction, useful for reporting and analysis at the store level.',
        region STRING NOT NULL COMMENT 'Indicates the geographical area of the store, which can be important for regional sales analysis and market segmentation.',
        store_type STRING NOT NULL COMMENT 'Describes the category of the store (e.g., online, brick-and-mortar), helping to analyze performance across different retail formats.',
        sales_channel STRING NOT NULL COMMENT 'Identifies the method through which the sale was made (e.g., in-store, online), useful for understanding customer preferences and channel effectiveness.',
        customer_id STRING COMMENT 'A unique identifier for the customer making the purchase, enabling customer behavior analysis and targeted marketing efforts.'
    )
    USING DELTA
    COMMENT 'Retail transactions fact table'
    PARTITIONED BY (transaction_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    spark.sql(create_table_sql)
    
    # Add Primary Key constraint
    spark.sql(f"""
        ALTER TABLE {full_table_name} 
        ADD CONSTRAINT transactions_pk PRIMARY KEY (transaction_id)
    """)
    
    # Add Foreign Key constraints
    # FK to retail_articles table
    articles_table = f"{catalog}.{schema}.retail_articles"
    spark.sql(f"""
        ALTER TABLE {full_table_name} 
        ADD CONSTRAINT transactions_article_fk 
        FOREIGN KEY (article_id) REFERENCES {articles_table} (article_id)
    """)
    
    # FK to date_dimensions table (linking transaction_date to full_date)
    date_dim_table = f"{catalog}.{schema}.date_dimensions"
    spark.sql(f"""
        ALTER TABLE {full_table_name} 
        ADD CONSTRAINT transactions_date_fk 
        FOREIGN KEY (transaction_date) REFERENCES {date_dim_table} (full_date)
    """)
    
    # Add tags to the table (key-value format)
    spark.sql(f"ALTER TABLE {full_table_name} SET TAGS ('asda_articles' = 'silver_articles')")
    
    print(f"Table {full_table_name} created successfully with:")
    print(f"  - PK: transaction_id")
    print(f"  - FK: article_id -> {articles_table}(article_id)")
    print(f"  - FK: transaction_date -> {date_dim_table}(full_date)")
    print(f"  - Tag: asda_articles=silver_articles")
    
    # Load data from CSV if path is provided
    if csv_path:
        df = spark.read.csv(
            csv_path,
            header=True,
            schema=transactions_schema,
            dateFormat="dd-MM-yyyy",
        )
        df.write.mode("overwrite").partitionBy("transaction_date").saveAsTable(full_table_name)
        print(f"Data loaded into {full_table_name} from {csv_path}")
    
    return full_table_name


def load_transactions_from_csv(
    spark,
    csv_path: str,
    catalog: str = "asda_arch_day",
    schema: str = "retail",
    table_name: str = "transactions",
):
    """
    Load transactions data from a CSV file into an existing table.
    
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
        schema=transactions_schema,
        dateFormat="dd-MM-yyyy",
    )
    
    df.write.mode("overwrite").partitionBy("transaction_date").saveAsTable(full_table_name)
    print(f"Data loaded into {full_table_name} from {csv_path}")
    
    # Display record count
    count = spark.table(full_table_name).count()
    print(f"Total records: {count}")


def create_transactions_table_with_date_key(
    spark,
    catalog: str = "asda_arch_day",
    schema: str = "retail",
    table_name: str = "transactions",
):
    """
    Alternative table creation with date_key for joining with date_dimensions.
    This version adds a date_key column in YYYYMMDD format.
    
    Args:
        spark: SparkSession object
        catalog: Catalog name in Unity Catalog
        schema: Schema/database name
        table_name: Name of the table to create
    """
    
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    # Create catalog and schema if they don't exist
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    
    # Drop table if exists
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    
    # Create the table with date_key for easier joins
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        transaction_id STRING NOT NULL COMMENT 'A unique identifier for each transaction, allowing for tracking and referencing specific sales events.',
        basket_id STRING NOT NULL COMMENT 'Identifies the shopping basket associated with the transaction, which can be useful for analyzing customer purchasing patterns.',
        article_id STRING NOT NULL COMMENT 'Represents the unique identifier for each product sold in the transaction, facilitating inventory and sales analysis.',
        transaction_date DATE NOT NULL COMMENT 'The date when the transaction occurred, essential for time-based sales analysis and trend identification.',
        date_key BIGINT COMMENT 'Date key for joining with date_dimensions (YYYYMMDD format).',
        quantity BIGINT NOT NULL COMMENT 'Indicates the number of units sold in the transaction, providing insight into product demand and sales volume.',
        unit_price DOUBLE NOT NULL COMMENT 'The price per unit of the product at the time of the transaction, useful for calculating revenue and pricing strategies.',
        discount_pct BIGINT NOT NULL COMMENT 'The percentage discount applied to the transaction, which helps in evaluating promotional effectiveness and pricing adjustments.',
        total_amount DOUBLE NOT NULL COMMENT 'The total monetary value of the transaction after applying any discounts, crucial for revenue analysis.',
        store_id STRING NOT NULL COMMENT 'A unique identifier for the store where the transaction took place, aiding in store performance evaluation.',
        store_name STRING NOT NULL COMMENT 'The name of the store involved in the transaction, useful for reporting and analysis at the store level.',
        region STRING NOT NULL COMMENT 'Indicates the geographical area of the store, which can be important for regional sales analysis and market segmentation.',
        store_type STRING NOT NULL COMMENT 'Describes the category of the store (e.g., online, brick-and-mortar), helping to analyze performance across different retail formats.',
        sales_channel STRING NOT NULL COMMENT 'Identifies the method through which the sale was made (e.g., in-store, online), useful for understanding customer preferences and channel effectiveness.',
        customer_id STRING COMMENT 'A unique identifier for the customer making the purchase, enabling customer behavior analysis and targeted marketing efforts.'
    )
    USING DELTA
    COMMENT 'Retail transactions fact table with date_key for dimension joins'
    PARTITIONED BY (date_key)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    spark.sql(create_table_sql)
    
    # Add Primary Key constraint
    spark.sql(f"""
        ALTER TABLE {full_table_name} 
        ADD CONSTRAINT transactions_pk PRIMARY KEY (transaction_id)
    """)
    
    # Add Foreign Key constraints
    # FK to retail_articles table
    articles_table = f"{catalog}.{schema}.retail_articles"
    spark.sql(f"""
        ALTER TABLE {full_table_name} 
        ADD CONSTRAINT transactions_article_fk 
        FOREIGN KEY (article_id) REFERENCES {articles_table} (article_id)
    """)
    
    # FK to date_dimensions table (linking date_key to date_key)
    date_dim_table = f"{catalog}.{schema}.date_dimensions"
    spark.sql(f"""
        ALTER TABLE {full_table_name} 
        ADD CONSTRAINT transactions_date_fk 
        FOREIGN KEY (date_key) REFERENCES {date_dim_table} (date_key)
    """)
    
    # Add tags to the table (key-value format)
    spark.sql(f"ALTER TABLE {full_table_name} SET TAGS ('asda_articles' = 'silver_articles')")
    
    print(f"Table {full_table_name} created successfully with date_key column and:")
    print(f"  - PK: transaction_id")
    print(f"  - FK: article_id -> {articles_table}(article_id)")
    print(f"  - FK: date_key -> {date_dim_table}(date_key)")
    print(f"  - Tag: asda_articles=silver_articles")
    
    return full_table_name


def load_transactions_with_date_key(
    spark,
    csv_path: str,
    catalog: str = "asda_arch_day",
    schema: str = "retail",
    table_name: str = "transactions",
):
    """
    Load transactions data and compute date_key from transaction_date.
    Converts DATE to YYYYMMDD bigint format.
    
    Args:
        spark: SparkSession object
        csv_path: Path to the CSV file
        catalog: Catalog name in Unity Catalog
        schema: Schema/database name
        table_name: Name of the table
    """
    from pyspark.sql.functions import col, date_format
    
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    df = spark.read.csv(
        csv_path,
        header=True,
        schema=transactions_schema,
        dateFormat="dd-MM-yyyy",
    )
    
    # Convert transaction_date to date_key (YYYYMMDD)
    df_with_key = df.withColumn(
        "date_key",
        date_format(col("transaction_date"), "yyyyMMdd").cast(LongType())
    )
    
    df_with_key.write.mode("overwrite").partitionBy("date_key").saveAsTable(full_table_name)
    print(f"Data loaded into {full_table_name} from {csv_path} with date_key")
    
    # Display record count
    count = spark.table(full_table_name).count()
    print(f"Total records: {count}")


# Example usage in Databricks notebook:
# create_transactions_table(spark, catalog="asda_arch_day", schema="retail")
# load_transactions_from_csv(spark, "dbfs:/mnt/data/transactions_dataset.csv")

# Or with date_key for easier joins:
# create_transactions_table_with_date_key(spark, catalog="asda_arch_day", schema="retail")
# load_transactions_with_date_key(spark, "dbfs:/mnt/data/transactions_dataset.csv")
