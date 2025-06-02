# Azure-Spark-Databricks-Optimization

## Description

The goal of the project was to build fully parameterized, reusable, and scalable pipelines in Azure Data Factory, based on Databricks notebooks and compressed Parquet data stored in Azure Data Lake, following the OLAP model and medallion architecture.
As part of the solution, a range of optimization techniques were applied, including:
– Lazy evaluation,
– Adaptive Query Execution (AQE) and Cost-Based Optimization (CBO) hints,
– analysis of logical and physical plans,
– partition pruning and partition discovery,
– caching and persist mechanisms,
– repartition and coalesce operations,
based on metrics and statistics available in the Spark UI.
