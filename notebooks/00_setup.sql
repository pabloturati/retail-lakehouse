-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 — Setup
-- MAGIC Creates the Unity Catalog schema and the Volume that holds the raw H&M CSVs.
-- MAGIC Uses the `workspace` catalog that Databricks Free Edition provides.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.retail
COMMENT 'H&M retail lakehouse: bronze, silver and gold tables';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.retail.raw
COMMENT 'Raw H&M CSVs as downloaded from Kaggle (not redistributed)';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Upload `transactions_train.csv` and `articles.csv` to `/Volumes/workspace/retail/raw/` before running `01_bronze`.

-- COMMAND ----------

LIST '/Volumes/workspace/retail/raw/';
