# retail-lakehouse

A bronze → silver → gold lakehouse on Databricks over the public H&M retail dataset, built with
PySpark and Delta Lake in Unity Catalog, with an assistant over the result.

## Status — 2026-10-20

### Done

| Table | Rows |
|---|---|
| `workspace.retail.bronze_transactions` | 31,788,324 |
| `workspace.retail.bronze_articles` | 105,542 |
| `workspace.retail.silver_transactions` | 31,788,324 |
| `workspace.retail.silver_articles` | 105,542 |

- Unity Catalog schema `workspace.retail` and Volume `workspace.retail.raw` for the raw CSVs
  (`notebooks/00_setup.sql`).
- **Bronze** (`notebooks/01_bronze.py`) — both CSVs read with an **explicit `StructType`**, not
  `inferSchema`, and written to Delta. Each row carries `_source_file` (from
  `_metadata.file_path`) and `_ingested_at`. The full dataset is used; nothing is sampled.
- **Silver** (`notebooks/02_silver.py`) — cleaning rules below, each measured against bronze
  before being applied.

### Next

- Gold aggregates: sales per article per week, top departments, repeat customers.
- Incremental loads with `MERGE INTO`, and data-quality expectations.
- Delta features: time travel, `OPTIMIZE`, `DESCRIBE HISTORY`, schema enforcement vs evolution.
- Embeddings over `detail_desc` for retrieval over the product catalog.
- An assistant that routes sales questions to SQL over gold and catalog questions to retrieval.
- MLflow tracing and an evaluation set of 15–20 questions.
- `docs/NOTES-internals.md`: query plans, shuffle and skew notes from local Spark.

## Silver cleaning rules

Transactions span **2018-09-20 to 2020-09-22**.

Every rule below was measured against all 31,788,324 bronze rows first, then applied. The
measurements are printed in `notebooks/02_silver.py` and are reproduced here.

1. **`t_dat` becomes a real `DATE`.** Rows whose date is null or fails to parse are dropped — a
   sale with no date cannot be aggregated by time, which is what gold does.
   *Measured: 0 null, 0 unparseable.*
2. **Rows with a null `customer_id` or `article_id` are dropped.** They join to nothing.
   *Measured: 0 of each.*
3. **Rows with a null or non-positive `price` are dropped.** This dataset contains no returns or
   refunds, so a zero or negative price would be corruption rather than a credit note.
   *Measured: 0 null, 0 non-positive.*
4. **Duplicate rows are kept.** A customer buying two of the same article on the same day
   produces two byte-identical rows, and the source has no quantity, line number or order id, so
   an identical row cannot be distinguished from a real second unit. A reflexive
   `dropDuplicates()` would have deleted **2,974,905 rows (9.36%)** of real sales.
   *Measured: 31,788,324 rows, 28,813,419 distinct — kept as is.*
5. **`sales_channel_id` gains a readable `sales_channel`** label (`1` → `store`, `2` → `online`).
   The original id is kept, so nothing is lost. *Measured: 0 rows outside {1, 2}.*
6. **`article_id` stays a `STRING`,** in bronze and silver. Values such as `0663713001` have a
   leading zero that is part of the identifier; reading it as a number destroys it.
7. **Articles: one row per `article_id`**, and `detail_desc` is trimmed with `""` normalised to
   `NULL`, because that column is the text that gets embedded for retrieval and a blank string is
   not a description. *Measured: 0 duplicate ids, 0 null ids, 0 empty descriptions, 416 already
   null.*

**Rules 1–3 and 5 each matched zero rows.** The Kaggle extract is already internally consistent,
so silver dropped nothing: 31,788,324 rows in, 31,788,324 out. The rules are kept as written
guards rather than removed, and the counts are published rather than described as a cleanup that
did not happen. What silver genuinely changes is the `t_dat` type, the added `sales_channel`
label, and the deliberate decision in rule 4.

**`price` is left exactly as it is.** It is an H&M-scaled value, not a currency amount, so it is
not converted or relabelled.

## How to run

Built on **Databricks Free Edition**, serverless compute. No cluster configuration is involved.

1. Download the two CSVs from Kaggle (see *Data* below).
2. Run `notebooks/00_setup.sql` — creates the schema `workspace.retail` and the Volume
   `workspace.retail.raw`.
3. Upload `transactions_train.csv` (3.5 GB) and `articles.csv` to
   `/Volumes/workspace/retail/raw/` via **Catalog → workspace → retail → Volumes → raw → Upload
   to this volume**.
4. Run `notebooks/01_bronze.py`, then `notebooks/02_silver.py`, in that order.

The notebooks are also committed as `.ipynb` with their output cells, so the printed row counts
and profile numbers can be read without running anything.

## Data

[H&M Personalized Fashion Recommendations](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations)
(Kaggle competition). `transactions_train.csv` and `articles.csv` only.

**The data is not committed.** The competition rules restrict redistribution. Download it from
Kaggle and upload it to the Volume as described above.

## Fallbacks used

None so far. Bronze and silver use Unity Catalog, Volumes and Delta directly on Free Edition
serverless, with no substitutions.

Free Edition may not offer Vector Search, Model Serving, Databricks Apps or Lakeflow Declarative
Pipelines. If a fallback is needed for a later stage, it will be named here.
