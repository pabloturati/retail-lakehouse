# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze
# MAGIC Lands the raw H&M CSVs as Delta tables, unchanged.
# MAGIC
# MAGIC Every column is read with an **explicit schema** rather than `inferSchema`. Inference
# MAGIC would read `article_id` (`0663713001`) as a number and destroy the leading zero, and it
# MAGIC costs an extra pass over 3.5 GB to guess what we already know.
# MAGIC
# MAGIC Bronze does no cleaning. `t_dat` stays a string here and is parsed in silver, so that the
# MAGIC bronze table is a faithful copy of the file and any unparseable date is visible rather
# MAGIC than silently null.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

CATALOG = "workspace"
SCHEMA = "retail"
VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transactions

# COMMAND ----------

transactions_schema = StructType([
    StructField("t_dat", StringType(), nullable=True),            # 'YYYY-MM-DD'; parsed in silver
    StructField("customer_id", StringType(), nullable=True),      # 64-char hex
    StructField("article_id", StringType(), nullable=True),       # keeps leading zeros
    StructField("price", DoubleType(), nullable=True),            # H&M-scaled, not currency
    StructField("sales_channel_id", IntegerType(), nullable=True) # 1 = store, 2 = online
])

transactions = (
    spark.read
    .schema(transactions_schema)
    .option("header", True)
    .csv(f"{VOLUME}/transactions_train.csv")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

(transactions.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("bronze_transactions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Articles

# COMMAND ----------

articles_schema = StructType([
    StructField("article_id", StringType(), nullable=True),
    StructField("product_code", StringType(), nullable=True),
    StructField("prod_name", StringType(), nullable=True),
    StructField("product_type_no", IntegerType(), nullable=True),
    StructField("product_type_name", StringType(), nullable=True),
    StructField("product_group_name", StringType(), nullable=True),
    StructField("graphical_appearance_no", IntegerType(), nullable=True),
    StructField("graphical_appearance_name", StringType(), nullable=True),
    StructField("colour_group_code", StringType(), nullable=True),
    StructField("colour_group_name", StringType(), nullable=True),
    StructField("perceived_colour_value_id", IntegerType(), nullable=True),
    StructField("perceived_colour_value_name", StringType(), nullable=True),
    StructField("perceived_colour_master_id", IntegerType(), nullable=True),
    StructField("perceived_colour_master_name", StringType(), nullable=True),
    StructField("department_no", IntegerType(), nullable=True),
    StructField("department_name", StringType(), nullable=True),
    StructField("index_code", StringType(), nullable=True),
    StructField("index_name", StringType(), nullable=True),
    StructField("index_group_no", IntegerType(), nullable=True),
    StructField("index_group_name", StringType(), nullable=True),
    StructField("section_no", IntegerType(), nullable=True),
    StructField("section_name", StringType(), nullable=True),
    StructField("garment_group_no", IntegerType(), nullable=True),
    StructField("garment_group_name", StringType(), nullable=True),
    StructField("detail_desc", StringType(), nullable=True)       # free text; the retrieval corpus
])

articles = (
    spark.read
    .schema(articles_schema)
    .option("header", True)
    .option("multiLine", True)      # some detail_desc values contain newlines
    .option("quote", '"')
    .option("escape", '"')
    .csv(f"{VOLUME}/articles.csv")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

(articles.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("bronze_articles"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row counts
# MAGIC These numbers go in the README. They are read back from the written tables, not from the
# MAGIC DataFrames above, so they describe what is actually stored.

# COMMAND ----------

for table in ["bronze_transactions", "bronze_articles"]:
    print(f"{CATALOG}.{SCHEMA}.{table}: {spark.table(table).count():,} rows")

# COMMAND ----------

display(spark.table("bronze_transactions").limit(5))

# COMMAND ----------

display(spark.table("bronze_articles").limit(5))
