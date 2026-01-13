"""
Databricks schema creation script for retail_articles table.
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
# spark = SparkSession.builder.appName("RetailArticlesSchema").getOrCreate()

# Define the schema for retail_articles
retail_articles_schema = StructType([
    StructField("article_id", StringType(), False),
    StructField("status", StringType(), False),
    StructField("valid_from", DateType(), False),
    StructField("valid_to", DateType(), True),
    StructField("lifecycle_group", StringType(), False),
    StructField("department", StringType(), False),
    StructField("category", StringType(), False),
    StructField("subcategory", StringType(), False),
    StructField("brand", StringType(), False),
    StructField("article_name", StringType(), False),
    StructField("short_description", StringType(), True),
    StructField("supplier_id", StringType(), False),
    StructField("supplier_name", StringType(), False),
    StructField("country_of_origin", StringType(), False),
    StructField("base_uom", StringType(), False),
    StructField("net_content", StringType(), False),
    StructField("net_weight_kg", DoubleType(), False),
    StructField("base_cost", DoubleType(), False),
    StructField("list_price", DoubleType(), False),
    StructField("tax_rate_pct", DoubleType(), False),
    StructField("discountable", StringType(), False),
    StructField("in_stores", StringType(), False),
    StructField("in_ecommerce", StringType(), False),
    StructField("distribution_center_id", StringType(), False),
    StructField("distribution_center_name", StringType(), False),
    StructField("replenishment_type", StringType(), False),
    StructField("lead_time_days", LongType(), False),
])


def create_retail_articles_table(
    spark,
    catalog: str = "asda_arch_day",
    schema: str = "retail",
    table_name: str = "retail_articles",
    csv_path: str = None,
):
    """
    Create the retail_articles table in Databricks Unity Catalog.
    
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
        article_id STRING NOT NULL COMMENT 'Unique identifier for each retail article, essential for tracking and managing inventory.',
        status STRING NOT NULL COMMENT 'Indicates the current status of the article, such as active, discontinued, or out of stock.',
        valid_from DATE NOT NULL COMMENT 'The date from which the article information is considered valid, useful for tracking changes over time.',
        valid_to DATE COMMENT 'The date until which the article information remains valid, helping to manage lifecycle and updates.',
        lifecycle_group STRING NOT NULL COMMENT 'Categorizes the article based on its lifecycle stage, aiding in inventory and sales strategies.',
        department STRING NOT NULL COMMENT 'Identifies the department under which the article is categorized, facilitating organization and reporting.',
        category STRING NOT NULL COMMENT 'Specifies the broader category of the article, useful for grouping similar products.',
        subcategory STRING NOT NULL COMMENT 'Further refines the categorization of the article, allowing for more detailed analysis and reporting.',
        brand STRING NOT NULL COMMENT 'The brand associated with the article, important for brand management and marketing strategies.',
        article_name STRING NOT NULL COMMENT 'The name of the article, which is used for identification and customer-facing purposes.',
        short_description STRING COMMENT 'A brief description of the article, providing essential information at a glance.',
        supplier_id STRING NOT NULL COMMENT 'Unique identifier for the supplier of the article, important for supply chain management.',
        supplier_name STRING NOT NULL COMMENT 'The name of the supplier, which helps in identifying and managing supplier relationships.',
        country_of_origin STRING NOT NULL COMMENT 'Indicates where the article is produced, relevant for compliance and sourcing decisions.',
        base_uom STRING NOT NULL COMMENT 'The base unit of measure for the article, crucial for inventory management and sales calculations.',
        net_content STRING NOT NULL COMMENT 'Details the net content of the article, which is important for packaging and customer information.',
        net_weight_kg DOUBLE NOT NULL COMMENT 'The weight of the article in kilograms, useful for shipping and logistics considerations.',
        base_cost DOUBLE NOT NULL COMMENT 'The cost price of the article, essential for pricing strategies and profit calculations.',
        list_price DOUBLE NOT NULL COMMENT 'The retail price at which the article is offered, important for sales and marketing efforts.',
        tax_rate_pct DOUBLE NOT NULL COMMENT 'The applicable tax rate percentage for the article, necessary for pricing and compliance.',
        discountable STRING NOT NULL COMMENT 'Indicates whether the article is eligible for discounts, relevant for promotional strategies.',
        in_stores STRING NOT NULL COMMENT 'Specifies if the article is available in physical stores, aiding in inventory and sales channel management.',
        in_ecommerce STRING NOT NULL COMMENT 'Indicates if the article is available for purchase online, important for e-commerce strategy.',
        distribution_center_id STRING NOT NULL COMMENT 'Unique identifier for the distribution center managing the article, important for logistics.',
        distribution_center_name STRING NOT NULL COMMENT 'The name of the distribution center, useful for operational management and coordination.',
        replenishment_type STRING NOT NULL COMMENT 'Describes the method of replenishment for the article, aiding in inventory planning.',
        lead_time_days BIGINT NOT NULL COMMENT 'The number of days required to replenish the article, crucial for inventory management and planning.'
    )
    USING DELTA
    COMMENT 'Retail articles master data table'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
    
    spark.sql(create_table_sql)
    
    # Add Primary Key constraint
    spark.sql(f"""
        ALTER TABLE {full_table_name} 
        ADD CONSTRAINT retail_articles_pk PRIMARY KEY (article_id)
    """)
    
    # Add tags to the table (key-value format)
    spark.sql(f"ALTER TABLE {full_table_name} SET TAGS ('asda_articles' = 'silver_articles')")
    
    print(f"Table {full_table_name} created successfully with PK: article_id, tag: asda_articles=silver_articles")
    
    # Load data from CSV if path is provided
    if csv_path:
        df = spark.read.csv(
            csv_path,
            header=True,
            schema=retail_articles_schema,
            dateFormat="yyyy-MM-dd",
        )
        df.write.mode("overwrite").saveAsTable(full_table_name)
        print(f"Data loaded into {full_table_name} from {csv_path}")
    
    return full_table_name


def load_retail_articles_from_csv(
    spark,
    csv_path: str,
    catalog: str = "asda_arch_day",
    schema: str = "retail",
    table_name: str = "retail_articles",
):
    """
    Load retail_articles data from a CSV file into an existing table.
    
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
        schema=retail_articles_schema,
        dateFormat="yyyy-MM-dd",
    )
    
    df.write.mode("overwrite").saveAsTable(full_table_name)
    print(f"Data loaded into {full_table_name} from {csv_path}")
    
    # Display record count
    count = spark.table(full_table_name).count()
    print(f"Total records: {count}")


# Example usage in Databricks notebook:
# create_retail_articles_table(spark, catalog="asda_arch_day", schema="retail")
# load_retail_articles_from_csv(spark, "dbfs:/mnt/data/retail_articles_dataset_short.csv")
