# Azure-Spark-Databricks-Optimization

## Description

The goal of the project was to build fully parameterized, reusable, and scalable pipelines in Azure Data Factory, based on Databricks notebooks and compressed Parquet data stored in Azure Data Lake, following the OLAP model and medallion architecture.  
As part of the solution, a range of optimization techniques were applied, including:  
– Lazy evaluation  
– Adaptive Query Execution (AQE) and Cost-Based Optimization (CBO) hints  
– Analysis of logical and physical plans  
– Partition pruning and partition discovery  
– Caching and persist mechanisms  
– Repartition and coalesce operations  
All of the above were guided by runtime metrics and statistics available in the Spark UI.

![Architecture Diagram](https://github.com/ur64n/Azure-Spark-Databricks-Optimization/blob/main/img/join%20code%20in%20databricks%20and%20parametrized%20join%20activity%20in%20adf.png)

The code in the hr-parametrized-pipelines folder defines pipeline activities that support dynamic parameterization, enabling the construction of arbitrary datasets from fact and dimension tables, prepared for further analytical processing.
