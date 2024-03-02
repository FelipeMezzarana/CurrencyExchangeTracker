# Currency Exchange ETL

ETL process to:
 + Extract data about 273 Currencies exchange rates from a public API  
 + Organize, transform and store data in two tables (Dollar and Euro based rates) in a SQLite DB
 + Generate a Customized Excel report (decision-making tool)
 + Orchestrate a job with Airflow to recurrently run all steps

# Step 1: ETL

Data will be Extract from a public API https://github.com/fawazahmed0/exchange-api/tree/main. The API return a daily updated json with all the exchange rates for the for the selected base currency and date.

The script [update_currency_exchange.py](update_currency_exchange.py) is responsible for the whole ETL process. In short, this script contains functions to:
    
+ Create two tables (Dollar and Euro based rates) in the [SQLite DB](Database)
+ Query DB to find the last update date 
+ Run API requests for each day since the last update date 
+ Transform json data and insert the resulting DataFrame into the SQLite DB
    

# Step 2: Create Excel Report

With the data updated, we will run the script [create_report.py](create_report.py)

This script will generate a Excel report with one tab for each specified currency in the folder [Reports](Reports). Due to the scalability of the process, the desired currencies in the report can be easily added or removed from an argument found in the report_pipeline() function, which is the function responsible for running all the steps that generate the report.

The report will contain for each currency (tab):
+ The range rates of different time periods (e.g. last week, last month, etc.)
+ Two Line plots with the with the average, maximum and minimum values of the last 12 months (one for Dollar based rates and another for Euro)

it's easier to show than to describe:

![png](readme_files/report_print.PNG)


# Step 3: Orchestrate a Job/ Run the Pipeline

There are two ways to run the pipeline responsible for all stages of the process: 

The first and simplest is through the script [main.py](main.py) , running it from the CLI or from Docker [run.sh](run.sh) will execute all the steps in the pipeline.

The second is option is to orchestrate a job with Airflow. the DAG [dag_currency_exchange_etl.py](src/airflow/dag_currency_exchange_etl.py) will also run all the steps in the pipeline, it will only be necessary to have an active Airflow server. 


# Usage 

App:
```shell
# Run through Docker 
 ./run.sh 
# Run through Pytohn
 python3 -m src.main 
 ```

Linting:
```shell
./run_linting.sh 
```

Unit tests:
```shell
./run_unit_tests.sh 
```
Integration tests:
```shell
./run_integration_tests.sh 
```


# Structure

```bash
├── coverage
├── readme_files
├── src
│   ├── airflow
│   ├── database
│   ├── modules
│   └── reports
└── tests
    ├── integration
    └── unit
        └── sample_data
 ```

- `coverage` (not present in Github) is created when you run unit tests, and contains HTML for the code coverage. Specifically, open `coverage/index.html` to view the code coverage. 100% coverage is required.
- [readme_files](readme_files) Images used in readme.
- [src](src) Contains the application code.
- [src/airflow](src/airflow/) DAG file to run the app through Airflow.
- [src/database](src/database) SQLLite DB.
- [src/modules](src/pipeline/) Modules to run the ETL pipeline and generate the Excel report.
- [src/reports](src/reports/) Contains the generated Excel reports.
- [tests](tests) unit tests, integration tests and data samples.