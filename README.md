# Belvo Hackathon

The purpose of this case is to present a practical proposal for the treatment of data from the customers and deals information customers for [Belvo's](https://belvo.com/) portfolio .

> *(…) For this challenge, we want you to write a script to ingest some fake raw data, create
the necessary data models*
> 

# Architecture

![Untitled](Belvo%20Hackathon%20ec0eb7a31de443f9b6faf2fcc31ceeb8/Untitled.jpeg)

To solve this case, we will try to use best architecture practices and standards to carry out the data extraction, loading and transformation process.

A minimalist architecture is proposed through Snowflake.

Why Snowflake? In my experience, it is the most practical tool, quick to implement and comes with everything we need to focus on what is important, data analysis.

*“Snowflake is an analytics data warehouse delivered as 
        software as a service (SaaS). Snowflake’s data warehouse does not rely 
        on an existing database or “big data” software platform like Hadoop. 
        Snowflake’s data warehouse uses a new SQL database engine with a 
        unique architecture designed for the cloud.”*

For more information: 
        https://www.snowflake.com/workloads/data-warehouse-modernization/

# **Data Engineering**

As mentioned above, we will use Python to load the data, perform small transformations, and then data will be ingested into Snowflake. This is a process similar to ELT instead of ETL. Why? Because through Snowflake we have the ease of making the necessary queries to transform the data and the computing power that a local database (on my laptop) would not have.

Through a Notebook, functions will be developed to clean the file, shape it and send it to our Warehouse.

## ETL PROCESS

This Ingest process uses the Snowflake data warehouse to hold the external data.

### Definition of functions

The necessary functions for the ETL process will be defined below.

```python
import snowflake.connector
import os
from dotenv import load_dotenv
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

"""
Set up connection
"""
load_dotenv()
con = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database='BELVO_ANALYTICS',
    schema='RAW',

)

def get_data_from_snowflake(sql: str):
    """
    Function for get data from Snowflake, from a qwuery string
    :param sql: Query String
    :return: parsing Dataframe
    """
    cursor = con.cursor()
    cursor.execute(sql)
    result = cursor.fetch_pandas_all()
    return result

def map_cols_sf(df) -> str:
    """
    :param df:
    :return: columns remaping
    """
    columns_sql = ""
    for column in df.columns:
        if (df[column].dtype.name == "int" or df[column].dtype.name == "int64"):
            columns_sql = columns_sql + column + " int"
        elif df[column].dtype.name == "object":
            columns_sql = columns_sql + column + " varchar(16777216)"
        elif df[column].dtype.name == "datetime64[ns]":
            columns_sql = columns_sql + column + " datetime"
        elif df[column].dtype.name == "float64":
            columns_sql = columns_sql + column + " float8"
        elif df[column].dtype.name == "bool":
            columns_sql = columns_sql + column + " boolean"
        else:
            columns_sql = columns_sql + column + " varchar(16777216)"
        if df[column].name != df.columns[-1]:
            columns_sql = columns_sql + ",\n"

    return columns_sql

def send_data_to_snowflake(df, schema: str, name: str, hard_create, quote_identifiers=False):
    """
    Function for send data to Snowflake from a data Frame
    :param quote_identifiers: Optional for column String with space or special char
    :param hard_create: Optional to force creation of a table
    :param df: Input Data Frame
    :param schema: Schema to aim our table
    :param name: Table name
    """

    df_1 = df.copy()

    map_columns = map_cols_sf(df_1)

    ddl_create = f"CREATE OR REPLACE TABLE {schema.upper()}.{name.upper()} ({map_columns})"
    con.cursor().execute(ddl_create)

    write_pandas(con, df_1, table_name=name.upper(), schema=schema.upper(), quote_identifiers=quote_identifiers)

def put_data_in_snowflake( df, schema: str, table_name: str, ):
    df_1 = df.copy()
    return write_pandas(con, df_1, table_name=table_name.upper(), schema=schema.upper(), auto_create_table=True)
```

## Functions to handling file

```python
def csv_to_df(filename):
    """
    Function for read csv File
    :param filename:
    :return: Dataframe parsing from CSV
    """
    df = pd.read_csv(filename, sep=',', encoding='UTF8')
    return df

def standardize_columns(df):
    """
    Function for standardize columns of a dataframe
    :param df:
    :return: Columns standardized
    """

    df_1 = df.copy()
    new_columns = []
    count = 1
    for x in df_1.columns:
        x.lower()
        mt = x.lower().maketrans("/'.,:¿?/()ÁÉÍÓÚáéíóú", '          aeiouaeiou')
        new_column = x.translate(mt).strip().replace(" ", "_").upper()
        new_columns.append(new_column)

    return new_columns

def get_columns_duplicated(df):
    """
    Function to get a list of columns duplicated in a dataframe
    :param df:
    :return: List of columns duplicated in a dataframe
    """
    duplicated_columns = []
    for c in df.columns:

        if df.columns.to_list().count(c) > 1:
            duplicated_columns.append(c)

    return list(set(duplicated_columns))

def rename_duplicated_columns(df):
    """
    Function for rename duplicated values in a dataframe
    :param df: Input dataframe
    :return: Dataframe with renamed columns
    """
    cols = []
    count = 1
    duplicated_columns = get_columns_duplicated(df)

    for duplicated_column in duplicated_columns:
        for column in df.columns:
            if column == duplicated_column:
                cols.append(f'{duplicated_column}_{count}')
                count += 1
                continue
            cols.append(column)
        df.columns = cols
```

## Running Pipe Line

```python
file_names = os.listdir('sources')

for file in file_names:

    #1.  Read local File
    file_name = f"sources/{file}"
    df_raw = csv_to_df(file_name)

    #2. Handling column names
    df_raw.columns = standardize_columns(df_raw)

    #3. Check and handling duplicated columns
    rename_duplicated_columns(df_raw)
    table_name = file.replace('.csv','')

    #4. Send raw data to Snowflake :3
    put_data_in_snowflake(df_raw, 'RAW',table_name )
    print("Table ",table_name, "  was uploaded to Snowflake!")

```

# Data Modeling

The data modeling will be done with [DBT](https://www.getdbt.com/product/what-is-dbt/) (Data build tool) 

> *dbt is an open source command line tool that helps analysts and engineers transform the data in their warehouse more effectively. It started at RJMetrics in 2016 as a solution to add basic transformation capabilities to Stitch.*
> 

For modeling, it is proposed to have a decoupled distribution, which allows scalability in any direction, low coupling and high cohesion of the models. It will be done as shown below.

![Untitled](Belvo%20Hackathon%20ec0eb7a31de443f9b6faf2fcc31ceeb8/Untitled%201.jpeg)

At each stage, the models were made with their respective tests, also with seeds, to validate the status of the countries. separating each layer. For more detail check the models/marts directory

## DBT Structure

![Untitled](Belvo%20Hackathon%20ec0eb7a31de443f9b6faf2fcc31ceeb8/Untitled.png)

![Untitled](Belvo%20Hackathon%20ec0eb7a31de443f9b6faf2fcc31ceeb8/Untitled%201.png)

## DAG Lineage

The flow of data, as shown below at each stage, from the data sources, Staging, Warehouse and Reporting. A seed is also used to adapt the values of countries that can be grouped.

![Untitled](Belvo%20Hackathon%20ec0eb7a31de443f9b6faf2fcc31ceeb8/Untitled.gif)

## OLAP Modeling

> OLAP (online analytical processing) is a computing method that enables users to easily and selectively [extract](https://www.techtarget.com/searchbusinessanalytics/answer/Examining-different-data-access-methods-OLAP-and-data-mining)  and query data in order to analyze it from different points of view. OLAP business intelligence queries often aid in trends analysis, financial reporting, [sales forecasting](https://www.techtarget.com/searchcustomerexperience/definition/sales-forecast), budgeting and other planning purposes.
> 

In order to optimize the use of the Warehouse Layer and avoid data redundancy, an OLAP model is proposed, in order to obtain data between different tables, in a FN1 and FN2.

![Untitled](Belvo%20Hackathon%20ec0eb7a31de443f9b6faf2fcc31ceeb8/Untitled%202.png)

## Model Deployment in Snowflake

14 models were deployed, with 14 different tests that try to verify the integrity of the data in each stage, unit tests such as verification of duplicate values and non-null fields, as well as expected values for columns such as country code.

![Untitled](Belvo%20Hackathon%20ec0eb7a31de443f9b6faf2fcc31ceeb8/Untitled%203.png)

# Reporting

Through [Power Bi](https://powerbi.microsoft.com/es-es/desktop/), we will create a report that will help us analyze our data.

One of the advantages of using an analytical database is the ease of connecting through reporting tools such as Power BI.

Power bi has a native connector that allows us to do this.

### Models Loades into PowerBI

![Untitled](Belvo%20Hackathon%20ec0eb7a31de443f9b6faf2fcc31ceeb8/Untitled%204.png)

# Report 📊

In this URL, you can find the report Online 

[https://app.powerbi.com/view?r=eyJrIjoiYzQzZGJlOTctYzg1Zi00OTlmLWI5ZmEtOTM0NDRkNjcyNGE4IiwidCI6Ijc0YzBjMjUwLTFjNzctNDA1ZC05YjFlLTlhYzFmNTA4YWJlMyIsImMiOjR9](https://app.powerbi.com/view?r=eyJrIjoiYzQzZGJlOTctYzg1Zi00OTlmLWI5ZmEtOTM0NDRkNjcyNGE4IiwidCI6Ijc0YzBjMjUwLTFjNzctNDA1ZC05YjFlLTlhYzFmNTA4YWJlMyIsImMiOjR9)

The main idea of this report is to be able to answer questions about the behavior of Deals. Their states, and key factors that influence their amount or state.

![Untitled](Belvo%20Hackathon%20ec0eb7a31de443f9b6faf2fcc31ceeb8/Untitled%201.gif)

# Findings

[https://docs.google.com/presentation/d/1frh7GMdECX6Jmm1J4pIoutddmIdl7O6XcG_Q50u57-I/edit?usp=sharing](https://docs.google.com/presentation/d/1frh7GMdECX6Jmm1J4pIoutddmIdl7O6XcG_Q50u57-I/edit?usp=sharing)