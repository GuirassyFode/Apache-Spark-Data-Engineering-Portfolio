# ❄️ PySpark to Snowflake Integration Guide

> 📚 **Course Reference:** [Introduction to Data Engineering with Spark, Hadoop, and Snowflake](https://www.coursera.org/learn/spark-hadoop-snowflake-data-engineering) — Coursera
> > Applied hands-on implementation from the "Writing to Snowflake" module.
> >
> > ---
> >
> > ## Overview
> >
> > This guide documents how to connect a PySpark environment to Snowflake and write a Spark DataFrame into a Snowflake table using the `snowflake-connector-python` library.
> >
> > **Tech Stack:** PySpark · Snowflake · Python 3.12 · Jupyter Notebook
> >
> > ---
> >
> > ## Prerequisites
> >
> > ```bash
> > pip install snowflake-connector-python
> > pip install "snowflake-connector-python[pandas]"
> > ```
> >
> > ---
> >
> > ## 1. Find Your Snowflake Account Identifier
> >
> > Your account identifier is visible directly in your Snowflake URL:
> >
> > ```
> > https://app.snowflake.com/<region>/<account_locator>/
> > ```
> >
> > Use `<account_locator>.<region>` as your `account` value. Example:
> >
> > ```
> > skc83884.us-east-1
> > ```
> >
> > > ⚠️ Common mistake: using an incorrect or legacy account locator causes a `404 Not Found` error on the login endpoint.
> > >
> > > ---
> > >
> > > ## 2. Establish the Connection
> > >
> > > ```python
> > > import snowflake.connector
> > >
> > > conn = snowflake.connector.connect(
> > >     account="<account_locator>.<region>",   # e.g. skc83884.us-east-1
> > >     user=sf_login,
> > >     password=sf_password,
> > >     warehouse=sf_warehouse,
> > >     database=database_name,
> > >     schema=schema_name,
> > > )
> > >
> > > cur = conn.cursor()
> > > cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
> > > print(cur.fetchone())
> > > ```
> > >
> > > ---
> > >
> > > ## 3. Create a Table in Snowflake
> > >
> > > Use triple-quoted strings for multi-line SQL — single-quoted strings cause a `SyntaxError: unterminated string literal`:
> > >
> > > ```python
> > > create_table = """
> > > CREATE OR REPLACE TABLE FODEDEV.FODESCM.ACCOUNTS1 (
> > >     account_id   VARCHAR,
> > >     account_name VARCHAR,
> > >     created_date DATE
> > > )
> > > """
> > > cur.execute(create_table)
> > > ```
> > >
> > > ---
> > >
> > > ## 4. Write a Spark DataFrame to Snowflake
> > >
> > > ### Step 1 — Convert Spark DataFrame to Pandas
> > >
> > > `write_pandas` requires a **pandas** DataFrame, not a Spark DataFrame:
> > >
> > > ```python
> > > df_pandas = df.toPandas().reset_index(drop=True)
> > > ```
> > >
> > > > ⚠️ `.toPandas()` collects all data to the driver node. Use with caution on large datasets.
> > > >
> > > > ### Step 2 — Write to Snowflake
> > > >
> > > > ```python
> > > > from snowflake.connector.pandas_tools import write_pandas
> > > >
> > > > success, nchunks, nrows, _ = write_pandas(
> > > >     conn=conn,                  # connection object, NOT the cursor
> > > >     df=df_pandas,
> > > >     table_name="ACCOUNTS1",
> > > >     database="FODEDEV",
> > > >     schema="FODESCM",
> > > >     quote_identifiers=False     # avoids case-sensitivity column mismatch
> > > > )
> > > >
> > > > print(f"Success: {success}, Chunks: {nchunks}, Rows: {nrows}")
> > > > ```
> > > >
> > > > ---
> > > >
> > > > ## Common Errors & Fixes
> > > >
> > > > | Error | Cause | Fix |
> > > > |-------|-------|-----|
> > > > | `404 Not Found: post <account>.snowflakecomputing.com` | Wrong account identifier | Use `<locator>.<region>` from your Snowflake URL |
> > > > | `SyntaxError: unterminated string literal` | Multi-line SQL in single quotes | Use triple quotes `"""..."""` |
> > > > | `AttributeError: 'SnowflakeCursor' has no attribute '_session_parameters'` | Passed cursor instead of connection | Use `conn=conn`, not `conn=cur` |
> > > > | `NameError: name 'cnx' is not defined` | Variable name mismatch | Match the variable name used in your `connect()` call |
> > > > | `TypeError: object of type 'DataFrame' has no len()` | Passed Spark DataFrame instead of pandas | Call `.toPandas()` first |
> > > > | `invalid identifier '"account_id"'` | Column name case mismatch | Add `quote_identifiers=False` or uppercase columns with `df.columns.str.upper()` |
> > > >
> > > > ---
> > > >
> > > > ## Key Concepts
> > > >
> > > > - **`conn` vs `cur`** — Always pass the **connection** object (`conn`) to `write_pandas`, not the cursor (`cur`). The cursor is only for executing SQL.
> > > > - - **Column naming** — Snowflake stores column names in uppercase by default. Use `quote_identifiers=False` or ensure your DataFrame columns are uppercased.
> > > >   - - **Index reset** — Always call `.reset_index(drop=True)` after `.toPandas()` to avoid index-related warnings.
> > > >     - - **Auto-create table** — Skip the `CREATE TABLE` step by adding `auto_create_table=True` to `write_pandas`.
> > > >      
> > > >       - ---
> > > >
> > > > ## References
> > > >
> > > > - [Snowflake Connector for Python — Official Docs](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector)
> > > > - - [write_pandas API Reference](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#write_pandas)
> > > >   - - [Coursera: Spark, Hadoop & Snowflake Data Engineering](https://www.coursera.org/learn/spark-hadoop-snowflake-data-engineering)
