# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver
# MAGIC Cleans bronze into tables that are safe to aggregate.
# MAGIC
# MAGIC Every rule below is a decision, not a default. Each one is measured against bronze first,
# MAGIC printed, and only then applied — so the README can say how many rows each rule touched
# MAGIC instead of claiming the data was "cleaned".
# MAGIC
# MAGIC The one rule that is a decision *not* to act is deduplication. See `Duplicates` below.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "retail"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

bronze_tx = spark.table("bronze_transactions")
bronze_ar = spark.table("bronze_articles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profile transactions
# MAGIC One pass over 31.8M rows computing every number the rules below depend on. Done as a
# MAGIC single `agg` rather than one `count` per condition, so Spark scans the table once.

# COMMAND ----------

parsed_date = F.to_date("t_dat", "yyyy-MM-dd")

tx_profile = bronze_tx.agg(
    F.count(F.lit(1)).alias("rows"),
    F.sum(F.when(F.col("t_dat").isNull(), 1).otherwise(0)).alias("null_date"),
    F.sum(F.when(F.col("t_dat").isNotNull() & parsed_date.isNull(), 1).otherwise(0)).alias("unparseable_date"),
    F.sum(F.when(F.col("customer_id").isNull(), 1).otherwise(0)).alias("null_customer"),
    F.sum(F.when(F.col("article_id").isNull(), 1).otherwise(0)).alias("null_article"),
    F.sum(F.when(F.col("price").isNull(), 1).otherwise(0)).alias("null_price"),
    F.sum(F.when(F.col("price") <= 0, 1).otherwise(0)).alias("non_positive_price"),
    F.sum(F.when(~F.col("sales_channel_id").isin(1, 2), 1).otherwise(0)).alias("unknown_channel"),
    F.min(parsed_date).alias("first_date"),
    F.max(parsed_date).alias("last_date"),
).collect()[0]

for field in tx_profile.asDict():
    print(f"{field:>20}: {tx_profile[field]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Duplicates
# MAGIC A customer buying two of the same article on the same day produces two byte-identical
# MAGIC rows. The source has no line number, order id or quantity column, so an identical row is
# MAGIC indistinguishable from a real second unit — and dropping it would silently delete sales.
# MAGIC
# MAGIC **Decision: keep them.** The number below is the count of rows that would disappear under
# MAGIC a naive `dropDuplicates()`, which is why the rule is written down rather than assumed.

# COMMAND ----------

tx_rows = tx_profile["rows"]
tx_distinct = bronze_tx.select(
    "t_dat", "customer_id", "article_id", "price", "sales_channel_id"
).distinct().count()

print(f"rows:              {tx_rows:,}")
print(f"distinct rows:     {tx_distinct:,}")
print(f"would be dropped:  {tx_rows - tx_distinct:,}  ({(tx_rows - tx_distinct) / tx_rows:.2%}) — kept")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning rules — transactions
# MAGIC 1. `t_dat` becomes a real `DATE`. Rows whose date is null or does not parse are dropped:
# MAGIC    a sale with no date cannot be aggregated by time, which is what gold does.
# MAGIC 2. Rows with a null `customer_id` or `article_id` are dropped — they join to nothing.
# MAGIC 3. Rows with a null or non-positive `price` are dropped. The competition data has no
# MAGIC    returns or refunds, so a zero or negative price is corruption, not a credit note.
# MAGIC 4. `sales_channel_id` gains a readable `sales_channel` label (1 = store, 2 = online).
# MAGIC    The original id is kept so nothing is lost.
# MAGIC 5. Duplicates are kept, per the section above.
# MAGIC
# MAGIC `price` is left exactly as it is. It is an H&M-scaled value, **not** a currency amount;
# MAGIC converting it would be inventing a number.

# COMMAND ----------

silver_tx = (
    bronze_tx
    .withColumn("t_dat", parsed_date)
    .filter(F.col("t_dat").isNotNull())
    .filter(F.col("customer_id").isNotNull() & F.col("article_id").isNotNull())
    .filter(F.col("price").isNotNull() & (F.col("price") > 0))
    .withColumn(
        "sales_channel",
        F.when(F.col("sales_channel_id") == 1, "store")
         .when(F.col("sales_channel_id") == 2, "online")
         .otherwise("unknown"),
    )
    .select(
        "t_dat", "customer_id", "article_id", "price",
        "sales_channel_id", "sales_channel",
        "_source_file", "_ingested_at",
    )
)

(silver_tx.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_transactions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profile articles

# COMMAND ----------

ar_rows = bronze_ar.count()
ar_distinct_ids = bronze_ar.select("article_id").distinct().count()

ar_profile = bronze_ar.agg(
    F.sum(F.when(F.col("article_id").isNull(), 1).otherwise(0)).alias("null_article_id"),
    F.sum(F.when(F.col("detail_desc").isNull(), 1).otherwise(0)).alias("null_detail_desc"),
    F.sum(F.when(F.trim(F.col("detail_desc")) == "", 1).otherwise(0)).alias("empty_detail_desc"),
).collect()[0]

print(f"rows:               {ar_rows:,}")
print(f"distinct article_id:{ar_distinct_ids:,}")
print(f"duplicate ids:      {ar_rows - ar_distinct_ids:,}")
for field in ar_profile.asDict():
    print(f"{field:>20}: {ar_profile[field]:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning rules — articles
# MAGIC 1. Rows with a null `article_id` are dropped — the product key is the whole point of the table.
# MAGIC 2. One row per `article_id`. Unlike transactions, this is a product dimension: a repeated
# MAGIC    key is a defect, not a second sale. The count above says whether any exist.
# MAGIC 3. `detail_desc` is trimmed, and an empty string becomes `NULL`. This is the text that
# MAGIC    gets embedded for retrieval later, and `""` is not a description — making it NULL means
# MAGIC    the retrieval step can exclude it honestly instead of indexing blanks.
# MAGIC 4. `article_id` stays a STRING. Leading zeros are part of the identifier.

# COMMAND ----------

from pyspark.sql.window import Window

dedupe = Window.partitionBy("article_id").orderBy(F.col("_ingested_at").asc())

silver_ar = (
    bronze_ar
    .filter(F.col("article_id").isNotNull())
    .withColumn(
        "detail_desc",
        F.when(F.trim(F.col("detail_desc")) == "", None).otherwise(F.trim(F.col("detail_desc"))),
    )
    .withColumn("_rn", F.row_number().over(dedupe))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

(silver_ar.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_articles"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row counts
# MAGIC Bronze in, silver out, and what the rules removed. These numbers go in the README.

# COMMAND ----------

counts = {
    "bronze_transactions": spark.table("bronze_transactions").count(),
    "silver_transactions": spark.table("silver_transactions").count(),
    "bronze_articles": spark.table("bronze_articles").count(),
    "silver_articles": spark.table("silver_articles").count(),
}

for table, n in counts.items():
    print(f"{CATALOG}.{SCHEMA}.{table}: {n:,} rows")

print()
for name in ["transactions", "articles"]:
    dropped = counts[f"bronze_{name}"] - counts[f"silver_{name}"]
    pct = dropped / counts[f"bronze_{name}"]
    print(f"{name}: dropped {dropped:,} rows ({pct:.4%})")

# COMMAND ----------

display(spark.table("silver_transactions").limit(5))

# COMMAND ----------

display(spark.table("silver_articles").select(
    "article_id", "prod_name", "product_type_name", "colour_group_name", "detail_desc"
).limit(5))
