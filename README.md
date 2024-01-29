# Reddit Analysis Project

This project is designed to perform an Extract, Transform, and Load (ETL) process using Reddit API as the data source and DuckDB as the target database. It is built in a modular and extensible way to easily add more sources or targets in the future.

## Project Structure
```
reddit_threads/

|-- configs/
|   |-- config.ini
|-- connector/
|   |-- duckdb_connector.py
|   |-- reddit_connector.py
|-- extract/
|   |-- reddit_extractor.py
|-- load/
|   |-- duckdb_loader.py
|-- tests/
|-- |--__init__.py
|   |-- test_duckdb_loader.py
|   |-- test_reddit_data_transformer.py
|   |-- test_reddit_extractor.py
|-- transform/
|   |-- reddit_data_transformer.py
|-- .gitignore
|-- reddit_to_duckdb_etl.py
|-- run_duckdb_query.py
|-- requirements.txt
|-- README.md
```

## Dependencies

- Python 3.x
- PRAW (Python Reddit API Wrapper)
- DuckDB
- pandas
- pytest

Install the dependencies using the following command:

```bash
pip install -r requirements.txt
```

## Configuration

The `config.ini` file contains the configuration details for the Reddit API and DuckDB and other properties related to the pileline.\
Update the file as per requirement.

## How to Run the main data pipeline

Go to repo root location and replace your python.exe location.\
Execute the main script `reddit_to_duckdb_etl.py` using the following command:


```bash
[python_path] reddit_to_duckdb_etl.py --start_date 2023-01-01 --no_of_threads 2000
```

Replace `2023-01-01` and `2000` with the desired start_date and no_of_threads to pass as arguments.

## How to run query on duckdb

```bash
[python_path] run_duckdb_query.py
```
Add queries in `config.ini` file 

## How to Debug

To enable debug logs, modify the logging configuration in each module by setting the log level to `DEBUG` in `config.ini`.\

```
level = DEBUG
```

## Testing

## Running Tests

This project uses unit testcases for testing. To run all tests, use the following command:

```bash
pytest tests
```

Add more tests in the `tests` directory to ensure the functionality of the ETL process.

# SQL Query Analysis

## What is the average number of threads by day of week?
```
SELECT day,ROUND(COUNT(*)/COUNT(DISTINCT created_date),2) AS average_threads 
FROM reddit_posts 
GROUP BY day;
```
## Which thread has the highest engagement?

```
SELECT author, title, ups, downs, num_comments
FROM reddit_posts 
ORDER BY ups + downs + num_comments DESC 
LIMIT 1;
```
## Which thread is the most controversial one?

```
SELECT title, author, ups, downs, ROUND((ups * downs) / (ups + downs),2) AS controversial_score
FROM reddit_posts 
WHERE downs > 0 
ORDER by controversial_score DESC 
LIMIT 1;
```