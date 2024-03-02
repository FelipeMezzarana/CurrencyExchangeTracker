# Standard library
import logging
import sqlite3
from datetime import datetime, timedelta
from time import perf_counter
from typing import Optional

# Third party
import numpy as np
import pandas as pd
import requests

from .. import settings

update_currency = logging.getLogger("update_currency_exchange.py")


def create_table_currency_exchange(db_path: str, table_name: str) -> pd.DataFrame:
    """Create an empty table in a SQLite DB with a column for each currency available in the API.
    * API: https://github.com/fawazahmed0/currency-api
    """

    # Retrieve a json with all available currencies
    url_all_currencies = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies.json"
    resp = requests.get(url_all_currencies)
    all_currencies_json = resp.json()

    # Column names = currency_code
    currency_code_list = list(all_currencies_json.keys())
    # Create empty df
    empty_currency_df = pd.DataFrame(columns=["exchange_date"] + currency_code_list)
    # Define the data types for each col
    dtypes_dict = {"exchange_date": "DATE"}
    for col in currency_code_list:
        dtypes_dict.update({col: "FLOAT"})

    # Create table
    conn_lite = sqlite3.connect(db_path)
    empty_currency_df.to_sql(name=table_name, con=conn_lite, if_exists="fail", index=False, dtype=dtypes_dict)
    conn_lite.close()
    update_currency.info(f"Table {table_name} created!")

    return empty_currency_df


def last_exchange_date(db_path: str, table_name: str) -> datetime:
    """Return the last date added in the db."""

    conn_lite = sqlite3.connect(db_path)
    try:
        query = f"SELECT max(exchange_date) as last_update_date FROM {table_name}"
        max_date_df = pd.read_sql_query(query, conn_lite)
    except Exception:
        max_date_df = pd.DataFrame()
        update_currency.debug(Exception)
    conn_lite.close()

    if max_date_df.empty:
        # one year ago - Historical rates are only available for last 1 year
        last_update_date = datetime.today() - timedelta(days=365)  # .strftime('%Y-%m-%d')
    else:
        last_update_date_str = max_date_df.last_update_date[0]
        last_update_date = datetime.strptime(last_update_date_str, "%Y-%m-%d")

    update_currency.info(f"Last date updated in db: {last_update_date}")
    return last_update_date


def get_currency_exchange(
    db_path: str, table_name: str, based_currency: str, since_date: Optional[datetime] = None
) -> pd.DataFrame:
    """Return a DataFrame with all exchange rates for 273 currencies.

    based_currency -- may be any currency code
    (although the tables are created only for 'usd' and 'eur')
    since_date -- df will have a row for each day from "since date" to the current date.
    Default(identifies last date in db)
    """

    t_start = perf_counter()  # time counter
    # Retrieves the date of the last update in the table
    if not since_date:
        since_date = last_exchange_date(db_path, table_name)  # pragma: no cover

    if since_date and since_date.strftime("%Y-%m-%d") == datetime.today().strftime("%Y-%m-%d"):
        update_currency.info("Last Date Updated equal to Today (No new Recoeds)")
        return pd.DataFrame()

    currency_df = check_table(db_path, table_name)  # "Base df" with columns only

    apiVersion = settings.API_VERSION
    endpoint = f"currencies/{based_currency}.json"
    row_counter = 0
    request_date = None
    currencies_dict = None
    while request_date != datetime.today().strftime("%Y.%-m.%-d"):
        since_date = since_date + timedelta(days=1)  # Sum one day
        request_date = since_date.strftime("%Y.%m.%d")  # convert to str (request format)

        url = f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{request_date}/{apiVersion}/{endpoint}"
        req = requests.get(url)
        # data is missing for a few days, in these cases we will take the value of the previous day
        if req.status_code != 200:
            last_request_date = (since_date - timedelta(days=1)).strftime(
                "%Y.%-m.%-d"
            )  # convert to str (request format)
            url = (
                "https://cdn.jsdelivr.net/npm/@fawazahmed0/"
                f"currency-api@{last_request_date}/{apiVersion}/{endpoint}"
            )
            req = requests.get(url)
            if req.status_code != 200:
                raise Exception(f"Request Failed: {url}")
            else:
                currencies_dict = req.json()  # pragma: no cover
        else:
            currencies_dict = req.json()

        # Creates a list with the correct values of each currency for each column
        currency_values_list = [request_date]  # first col refers to request date
        for col in currency_df.columns[1:]:
            currency_value = currencies_dict.get(based_currency).get(col)
            if not currency_value:
                currency_values_list.append(np.nan)
            else:
                currency_values_list.append(currency_value)

        currency_df.loc[row_counter] = currency_values_list
        row_counter += 1

    t_end = perf_counter()
    update_currency.info(
        f"Df generated for {based_currency} based currency "
        f"with {len(currency_df)} recorded days.\n{t_end - t_start:.2f}s"
    )

    return currency_df


def check_table(db_path: str, table_name: str) -> pd.DataFrame:
    """Return 1 row sample of table.
    * Create table from scratch if not exist.
    """
    try:
        conn_lite = sqlite3.connect(db_path)
        query = f"SELECT * FROM {table_name} limit 1"
        df = pd.read_sql_query(query, conn_lite)
    except Exception:
        df = create_table_currency_exchange(db_path=db_path, table_name=table_name)
        update_currency.debug(Exception)

    return df


def insert_df_sqlite(df: pd.DataFrame, db_path: str, table_name: str) -> None:
    """Insert a df into the specified db and table."""

    table_sample = check_table(db_path, table_name)
    # Loc only cols that alredy exists
    table_sample_cols = table_sample.columns.tolist()
    df = df.loc[:, table_sample_cols]

    conn_lite = sqlite3.connect(db_path)
    df.to_sql(name=table_name, con=conn_lite, if_exists="append", index=False, method="multi")
    conn_lite.close()
    update_currency.info(f"{len(df)} rows inserted in db: {db_path} table: {table_name}")


def run(db_path: str, based_currency: str, table_prefix: str) -> None:
    """Update table for especified based_currency.
    * Create table and update if table not exist.
    """

    table_name = table_prefix + "_based_currency"

    # Update data
    currency_df = get_currency_exchange(db_path=db_path, table_name=table_name, based_currency=based_currency)
    # Update DB
    if not currency_df.empty:  # Update only if there are new values
        insert_df_sqlite(df=currency_df, db_path=db_path, table_name=table_name)  # pragma: no cover


def etl_pipeline(based_currency_mapping: dict, db_path: str) -> None:
    """Run ETL pipeline to update db."""

    for currency, table_prexix in based_currency_mapping.items():
        run(db_path=db_path, based_currency=currency, table_prefix=table_prexix)
