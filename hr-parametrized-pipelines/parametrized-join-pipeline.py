## Modules & Input Parameters ##

import json
from pyspark.sql.functions import col, broadcast
from pyspark.sql.utils import AnalysisException
from pyspark import StorageLevel

dbutils.widgets.text("basePath", "")
dbutils.widgets.text("factTable", "")
dbutils.widgets.text("dimTables", "")       # JSON: ["dim_date", "dim_time_type"]
dbutils.widgets.text("joinKeys", "")        # JSON: {"dim_date": "date_id", "dim_time_type": "time_type_id"}
dbutils.widgets.text("joinType", "inner")   # e.g. inner, left, right, broadcast_left
dbutils.widgets.text("columns", "")         # JSON: ["employee_id", "hours", "date"]
dbutils.widgets.text("debugMode", "save")   # save, explain, show
dbutils.widgets.text("persist", "")         # memory_only, disk_only, etc.
dbutils.widgets.text("repartitionCols", "") # JSON: ["employee_id"]
dbutils.widgets.text("coalesce", "")        # int

## Parse ##

def parse_json(raw, fallback=[]):
    try:
        return json.loads(raw) if raw else fallback
    except:
        return fallback

base_path = dbutils.widgets.get("basePath")
fact_table = dbutils.widgets.get("factTable")
dim_tables = parse_json(dbutils.widgets.get("dimTables"))
join_keys = json.loads(dbutils.widgets.get("joinKeys") or "{}")
join_type = dbutils.widgets.get("joinType")
columns = parse_json(dbutils.widgets.get("columns"))
debug_mode = dbutils.widgets.get("debugMode")
persist_level = dbutils.widgets.get("persist")
repartition_cols = parse_json(dbutils.widgets.get("repartitionCols"))
coalesce_num = dbutils.widgets.get("coalesce")

## Load Data ##

# Fact path
fact_path = f"abfss://silver@optimization0adls.dfs.core.windows.net/{base_path}/selected_fact_table/"
df_fact = spark.read.parquet(fact_path)

# Load dim
dim_dfs = {}
for dim in dim_tables:
    dim_path = f"abfss://silver@optimization0adls.dfs.core.windows.net/{base_path}/selected_dim_tables/{dim}/"
    dim_dfs[dim] = spark.read.parquet(dim_path)

## JOIN ## 

df = df_fact

for dim_name, join_key in join_keys.items():
    df_dim = dim_dfs.get(dim_name)

    if df_dim is None:
        print(f"Nie znaleziono danych dla tabeli wymiarów: {dim_name}")
        continue

    if join_type.startswith("broadcast_"):
        print(f"Join z broadcast: {dim_name} ON {join_key}")
        df = df.join(broadcast(df_dim), on=join_key, how="left")
    else:
        print(f"Join: {dim_name} ON {join_key} (typ: {join_type})")
        df = df.join(df_dim, on=join_key, how=join_type)

## Select/Persist/Repartition/Coalesce ##

if columns:
    df = df.select(*columns)

if persist_level:
    storage_level_map = {
        "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
        "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
        "DISK_ONLY": StorageLevel.DISK_ONLY,
        "OFF_HEAP": StorageLevel.OFF_HEAP
    }

    level = storage_level_map.get(persist_level.upper(), StorageLevel.MEMORY_AND_DISK)
    df = df.persist(level)

if repartition_cols:
    df = df.repartition(*[col(c) for c in repartition_cols])

if coalesce_num:
    df = df.coalesce(int(coalesce_num))

## Save/Explain/Show ##

# Trigger
if debug_mode.strip().lower() == "explain":
    df.limit(1).count()  # trigger action
    df.explain(extended=True)

elif debug_mode.strip().lower() == "show":
    display(df.limit(20))

elif debug_mode.strip().lower() == "save":
    output_path = f"abfss://silver@optimization0adls.dfs.core.windows.net/silver/{base_path}/joined/fact_with_dims/"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Dane zapisane do: {output_path}")

else:
    print(f"Nieznany tryb debugMode: {debug_mode}")
