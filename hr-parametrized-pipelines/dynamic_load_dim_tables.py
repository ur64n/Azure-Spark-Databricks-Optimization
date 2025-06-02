import json
from pyspark.sql.utils import AnalysisException
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import to_date, col

# 1. ADF Parameters
dbutils.widgets.text("tableName", "")
dbutils.widgets.text("columns", "")
dbutils.widgets.text("filter", "")
dbutils.widgets.text("basePath", "") 

table_name = dbutils.widgets.get("tableName")
columns_raw = dbutils.widgets.get("columns")
filter_expr = dbutils.widgets.get("filter")
base_folder = dbutils.widgets.get("basePath")

# 2. Column Parse
    try:
        columns = json.loads(columns_raw) if columns_raw else []
    except (json.JSONDecodeError, TypeError) as e:
        print(f"Błąd podczas parsowania 'columns': {e}")
        columns = []

# 3. Paths
    read_path = f"abfss://raw@optimization0adls.dfs.core.windows.net/hr_dim_table/{table_name}/"
    write_path = f"abfss://silver@optimization0adls.dfs.core.windows.net/{base_folder}/selected_dim_tables/{table_name}/"

# 4. Set time border (past 2 years)
    today = datetime.today().replace(day=1)
    start_date = (today - relativedelta(months=24)).strftime('%Y-%m-%d')

# 5. Load and process data
try:
    df = spark.read.parquet(read_path)

    if "date" in df.columns:
        df = df.filter(to_date(col("date"), "yyyy-MM-dd") >= start_date)

    if filter_expr:
        df = df.filter(filter_expr)

    if columns:
        df = df.select(*columns)

# 6. Save  
    df.write.mode("overwrite").parquet(write_path)
    print(f"Zapisano dane do: {write_path}")

except AnalysisException as e:
    print(f"Błąd podczas wczytywania danych: {e}")
    raise
