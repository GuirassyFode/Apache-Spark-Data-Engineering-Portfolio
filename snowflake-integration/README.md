# ❄️ PySpark to Snowflake Integration Guide

> 📚 **Course Reference:** [Introduction to Data Engineering with Spark, Hadoop, and Snowflake](https://www.coursera.org/learn/spark-hadoop-snowflake-data-engineering) — Coursera
> Hands-on implementation from the "Writing to Snowflake" module.

---

## Overview

End-to-end pipeline connecting **Apache Spark** to **Snowflake**: reading a CSV with PySpark, writing to a Snowflake table, and querying data back into a pandas DataFrame.

**Tech Stack:** PySpark 3.5.1 · Snowflake · Python 3.12 · Jupyter Notebook

---

## Prerequisites

```bash
pip install pyspark
pip install snowflake-connector-python
pip install "snowflake-connector-python[pandas]"
```

---

## Pipeline Architecture

```
accounts.csv  →  PySpark (read CSV)  →  .toPandas()  →  Snowflake ACCOUNTS1  →  fetch_pandas_all()  →  pandas DataFrame
```

---

## Step 1 — Start a Spark Session

```python
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Spark_Snowflake_Integration") \
    .getOrCreate()
```

---

## Step 2 — Load CSV Data with PySpark

```python
df = spark.read.option('header', 'true').csv('./data/accounts.csv')
df.show()
```

Output:
```
+----------+--------------+------------+--------+-----------+
|account_id|account_holder|account_type| balance|     branch|
+----------+--------------+------------+--------+-----------+
|      1001| Alice Johnson|    Checking| 5000.00|   New York|
|      1002|     Bob Smith|     Savings|15000.00|Los Angeles|
|      1003|   Carol White|    Checking| 3500.00|    Chicago|
|      1004|  David Brown|     Savings|25000.00|   New York|
|      1005|    Emma Davis|    Checking| 7200.00|     Boston|
+----------+--------------+------------+--------+-----------+
```

---

## Step 3 — Connect to Snowflake

```python
import os
import snowflake.connector

sf_account_id = 'skc83884.us-east-1'
sf_login      = os.environ['SF_LOGIN']
sf_password   = os.environ['SF_PASSWORD']
sf_warehouse  = 'FODE_COMPUTE_WH'
database_name = 'FODEDEV'
schema_name   = 'FODESCM'

conn = snowflake.connector.connect(
    account=sf_account_id,
    user=sf_login,
    password=sf_password,
    warehouse=sf_warehouse,
    database=database_name,
    schema=schema_name,
)

cur = conn.cursor()
cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
print(cur.fetchone())
# Output: ('FGUIRASSY', 'ACCOUNTADMIN', 'FODEDEV', 'FODESCM')
```

Store credentials as environment variables — never hardcode passwords in notebooks.

---

## Step 4 — Create Table in Snowflake

```python
create_table = """
CREATE OR REPLACE TABLE FODEDEV.FODESCM.ACCOUNTS1 (
    ACCOUNT_ID     NUMBER(38,0),
    ACCOUNT_HOLDER VARCHAR(16777216),
    ACCOUNT_TYPE   VARCHAR(16777216),
    BALANCE        NUMBER(38,2),
    BRANCH         VARCHAR(16777216)
)
"""
cur.execute(create_table)
```

Use triple-quoted strings for multi-line SQL. Single quotes cause SyntaxError: unterminated string literal.

---

## Step 5 — Write Spark DataFrame to Snowflake

```python
from snowflake.connector.pandas_tools import write_pandas

df_pandas = df.toPandas()
df_pandas.columns = df_pandas.columns.str.upper()

success, nchunks, nrows, _ = write_pandas(
    conn=conn,
    df=df_pandas,
    table_name="ACCOUNTS1",
    database="FODEDEV",
    schema="FODESCM",
)
```

---

## Step 6 — Read Data Back from Snowflake to Pandas

### Option A — fetch_pandas_all() (recommended)

```python
cur.execute("SELECT * FROM FODEDEV.FODESCM.ACCOUNTS1")
df_from_snowflake = cur.fetch_pandas_all()

print(df_from_snowflake.shape)  # (5, 5)
df_from_snowflake.head()
```

Output:
```
   ACCOUNT_ID ACCOUNT_HOLDER ACCOUNT_TYPE  BALANCE       BRANCH
0        1001  Alice Johnson     Checking   5000.0     New York
1        1002      Bob Smith      Savings  15000.0  Los Angeles
2        1003    Carol White     Checking   3500.0      Chicago
3        1004   David Brown      Savings  25000.0     New York
4        1005     Emma Davis     Checking   7200.0       Boston
```

### Option B — fetchall() row by row

```python
cs = cur.execute("SELECT * FROM FODEDEV.FODESCM.ACCOUNTS1")
data = cs.fetchall()
for row in data:
    print(row)
```

Output:
```
(1001, 'Alice Johnson', 'Checking', Decimal('5000.00'), 'New York')
(1002, 'Bob Smith', 'Savings', Decimal('15000.00'), 'Los Angeles')
(1003, 'Carol White', 'Checking', Decimal('3500.00'), 'Chicago')
(1004, 'David Brown', 'Savings', Decimal('25000.00'), 'New York')
(1005, 'Emma Davis', 'Checking', Decimal('7200.00'), 'Boston')
```

---

## Step 7 — Close Connections

```python
cur.close()
conn.close()
```

---

## Common Errors and Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| 404 Not Found on login endpoint | Wrong account identifier | Use locator.region format from your Snowflake URL |
| SyntaxError: unterminated string literal | Multi-line SQL in single quotes | Use triple quotes |
| AttributeError: SnowflakeCursor has no _session_parameters | Passed cursor to write_pandas | Use conn=conn not conn=cur |
| NameError: cnx is not defined | Variable name mismatch | Match variable name from connect() call |
| TypeError: DataFrame has no len() | Passed Spark DataFrame to write_pandas | Call .toPandas() first |
| invalid identifier account_id | Column name case mismatch | Call df.columns.str.upper() before writing |

---

## Key Concepts

- **conn vs cur** — Pass the connection object (conn) to write_pandas, not the cursor (cur). The cursor is for executing SQL only.
- **Column casing** — Snowflake stores columns in UPPERCASE by default. Always call df.columns.str.upper() before writing.
- **Credentials** — Use os.environ to load credentials safely instead of hardcoding them.
- **fetch_pandas_all()** — The most efficient way to pull Snowflake query results directly into a pandas DataFrame.
- **auto_create_table=True** — Add this to write_pandas to skip the manual CREATE TABLE step.

---

## References

- [Snowflake Connector for Python — Official Docs](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector)
- [write_pandas API Reference](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#write_pandas)
- [Coursera: Spark, Hadoop and Snowflake Data Engineering](https://www.coursera.org/learn/spark-hadoop-snowflake-data-engineering)
