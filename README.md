## Data Pipeline to process StreetEasy data
**Project Description**: A daily snapshot of user search history and related data is saved to S3. Each file represents a single date, as noted by the filename: `inferred_users.20180330.csv.gz`. Each line in each file represents a *unique user*, as identified by `id` column. Information on each user's searches and engagement is stored in `searches` column. Given this data,

**Data Description**: The source data resides in S3 `s3://streeteasy-data-exercise` and needs to be processed using a data pipeline to answer business questions.

**Data Pipeline design**:
The design of the pipeline can be summarized as:
- Extract data from source S3 location.
- Process and Transform it using python and custom Airflow operators.
- Load a clean dataset and intermediate artifacts to destination S3 location.
- Calculate summary statistics and load the summary stats into Redshift.

**Design Goals**:
Based on the requirements of our data consumers, our pipeline is required to adhere to the following guidelines:
* The DAG should not have any dependencies on past runs.
* On failure, the task is retried for 3 times.
* Retries happen every 5 minutes.
* Catchup is turned off.
* Do not email on retry.

**Pipeline Implementation**:

Apache Airflow is a Python framework for programmatically creating workflows in DAGs, e.g. ETL processes, generating reports, and retraining models on a daily basis. The Airflow UI automatically parses our DAG and creates a natural representation for the movement and transformation of data. A DAG simply is a collection of all the tasks you want to run, organized in a way that reflects their relationships and dependencies. A **DAG** describes *how* you want to carry out your workflow, and **Operators** determine *what* actually gets done.

By default, airflow comes with some simple built-in operators like `PythonOperator`, `BashOperator`, `DummyOperator` etc., however, airflow lets you extend the features of a `BaseOperator` and create custom operators. For this project, I developed two custom operators:

- **StreetEasyOperator**:
- **ValidSearchStatsOperator**:

**Pipeline Schedule and Data Partitioning**:
The events data residing on S3 is partitioned by *year* (2018) and *month* (11). Our task is to incrementally load the event json files, and run it through the entire pipeline to calculate song popularity and store the result back into S3. In this manner, we can obtain the top songs per day in an automated fashion using the pipeline. Please note, this is a trivial analyis, but you can imagine other complex queries that follow similar structure.

```bash
├── README.md
├── docker-compose.yml
└── street-easy
    ├── dags
    │   ├── create_postgres_table.py
    │   └── street_easy.py
    ├── plugins
    │   ├── __init__.py
    │   ├── helpers
    │   │   ├── __init__.py
    │   │   └── transforms.py
    │   └── operators
    │       ├── __init__.py
    │       ├── extract_and_transform_streeteasy.py
    │       └── valid_search_stats.py
    └── requirements.txt
```

*S3 user searches data from 20th January to 30th March*:
```bash
s3://streeteasy-data-exercise/
inferred_users.20180120.csv.gz
inferred_users.20180121.csv.gz
inferred_users.20180122.csv.gz
inferred_users.20180123.csv.gz
inferred_users.20180124.csv.gz
..
inferred_users.20180325.csv.gz
inferred_users.20180326.csv.gz
inferred_users.20180327.csv.gz
inferred_users.20180328.csv.gz
inferred_users.20180329.csv.gz
inferred_users.20180330.csv.gz
```

*S3 output of unique valid searches per day*:
```bash
s3://skuchkula-etl/
unique_valid_searches_20180120.csv
unique_valid_searches_20180121.csv
unique_valid_searches_20180122.csv
unique_valid_searches_20180123.csv
unique_valid_searches_20180214.csv
...
```

```bash
s3://skuchkula-etl/
valid_searches_20180120.csv
valid_searches_20180121.csv
valid_searches_20180122.csv
valid_searches_20180123.csv
valid_searches_20180214.csv
...
```

## Tasks

### Task 1: Create an ETL to process files. Recommend any alternative file formats or compression types you think would help with storage costs or provide downstream compute flexibility.
Here are some difficulties with CSV files:
- No defined schema: There are no data types included and column names beyond a header row.
- Nested data requires special handling:
-
In addition to these issues with using CSV file format, Spark has some specific problems when working with CSV data:
- CSV files are quite slow to import and parse.
- The files cannot be shared between workers during the import process.
- If no schema is defined, then all data must be read before a schema can be inferred.
- Spark has a feature known as *predicate pushdown* - which is an idea of ordering tasks to do the least amount of work. Example, *filtering* data prior to processing is one of the primary optimizations of predicate pushdown, this drastically reduces the amount of information that must be processed in large data sets. Unfortunately, we cannot filter the CSV data via predicate pushdown.
- Finally, Spark processes are often multi-step and may utilize an intermediate file representation. These representations allow data to be used later without regenerating the data from source.

**Parquet Format**: Parquet is a compressed columnar data format and is structured with data accessible in chunks that allows efficient read/write operations without processing the entire file. This structured format supports Spark's predicate pushdown functionality, thus providing significant performance improvement. Finally, parquet files automatically include schema information and handle data encoding. This is perfect for intermediary or on-disk representation of processed data. Note that parquet files are binary file format and can only be used with proper tools.
