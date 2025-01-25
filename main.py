from typing import Dict, Union
from datetime import datetime
from fastapi import FastAPI, HTTPException

import ccxt
import psycopg2
import uvicorn
from config import config

app = FastAPI()


@app.get("/price/{currency}")
async def price(currency: str) -> Dict[str, Union[str, float, str]]:
    """Get the last bid price for the given cryptocurrency paired with USDT.

    Args:
        currency (str, optional): Symbol of selected currency.

    Raises:
        HTTPException: Error while inserting into PostgreSQL.
        HTTPException: Could not find bid price for selected currency.

    Returns:
        Dict[str, Union[str, float, str]]: 
            currency: Shortcut of currency
            last_bid_price: Last bid price of selected currency.
            time: Time of the last measurement.
    """
    currency_symbol = f"{currency.upper()}/USDT"
    table_name = currency_symbol.replace("/", "_").lower()
    try:
        exchange = ccxt.kucoin()
        ticker = exchange.fetch_ticker(currency_symbol)
        last_bid_price = ticker.get("bid")
        timestamp_seconds = ticker.get("timestamp")/1_000
        date = datetime.fromtimestamp(timestamp_seconds).strftime('%Y-%m-%d %H:%M:%S')

        if last_bid_price is None:
            raise HTTPException(status_code=400, detail=f"Could not find bid price for {currency_symbol}")
        connection = None
        cursor = None
        try:
            connection = connect()
            cursor = connection.cursor()
            if not table_exist(table_name, cursor):
                create_table(table_name, cursor)
            insert_data_to_table(table_name, date, last_bid_price, cursor, connection)
        except (Exception, psycopg2.Error) as error:
            print("Error while inserting into PostgreSQL:", error)
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        return {"currency": currency,
                "last_bid_price": last_bid_price,
                "time": date}
    except ccxt.BaseError as e:
        raise HTTPException(status_code=400, detail=f"An error occurred with KuCoin API: {str(e)}")


def connect() -> psycopg2.extensions.connection:
    """Create connection to database.

    Returns:
        psycopg2.extensions.connection: Connection instance.
    """
    params = config()
    print("Connecting to postgreSQL datatbase...")
    connection = psycopg2.connect(**params)
    return connection


def table_exist(table_name: str, cursor: psycopg2.extensions.cursor) -> bool:
    """Check if table exist.

    Args:
        table_name (str): Name of table to check.
        cursor (psycopg2.extensions.cursor): Cursor to selected database.

    Returns:
        bool: True if table exist, else False.
    """
    query = '''
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = %s);
    '''
    cursor.execute(query, (table_name,))
    return cursor.fetchone()[0]


def create_table(table_name: str, cursor: psycopg2.extensions.cursor) -> None:
    """Create table with provided name.

    Args:
        table_name (str): Name of table to create.
        cursor (psycopg2.extensions.cursor): Cursor to selected database.
    """
    cursor.execute(f'''CREATE TABLE {table_name}
                    (id SERIAL PRIMARY KEY,
                    date TIMESTAMP WITHOUT TIME ZONE,
                    value NUMERIC);''')


def insert_data_to_table(table_name: str,
                         date: str,
                         last_bid_price: float,
                         cursor: psycopg2.extensions.cursor,
                         connection: psycopg2.extensions.connection) -> None:
    """Insert one record of data into selected table.

    Args:
        table_name (str): Name of table where to save data.
        date (str): Date where record was measured.
        last_bid_price (float): Last bid price of selected currency.
        cursor (psycopg2.extensions.cursor): Cursor to selected database.
        connection (psycopg2.extensions.connection): Connection to selected database.
    """
    insert_query = f'''
                    INSERT INTO {table_name} (date, value)
                    VALUES (%s, %s)
                    '''
    data_to_insert = (date, last_bid_price)
    cursor.execute(insert_query, data_to_insert)
    connection.commit()
    print("Record inserted successfully.")


@app.get("/price/history/{currency}")
async def price_history(currency: str) -> Dict[str, float]:
    """Get history prices of selected currency from database.

    Args:
        currency (str): Shortcut of currency.

    Raises:
        Exception: Table does not exist.
        HTTPException: ccxt.BaseError

    Returns:
        Dict[str, float]: Dates and values gained from selected table.
            date_1: value_1
            date_2: value_2
            date_3: value_3
            etc...
    """
    currency_symbol = f"{currency.upper()}/USDT"
    table_name = currency_symbol.replace("/", "_").lower()
    connection = None
    cursor = None
    try:
        connection = connect()
        cursor = connection.cursor()
        if not table_exist(table_name, cursor):
            raise Exception(f"Table {table_name} does not exist.")
        history_prices = get_history_prices(table_name, cursor)
    except ccxt.BaseError as e:
        raise HTTPException(status_code=400, detail=f"An error occurred with KuCoin API: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
    return history_prices


def get_history_prices(table_name: str, cursor: psycopg2.extensions.cursor) -> Dict[str, float]:
    """Extract history prices from selected table.

    Args:
        table_name (str): Name of table to extract data from.
        cursor (psycopg2.extensions.cursor): Cursor to selected database.

    Returns:
        Dict[str, float]: Dates and values gained from selected table.
            date_1: value_1
            date_2: value_2 
            date_3: value_3
            etc...
    """
    select_query = f'''
                    SELECT date, value
                    FROM {table_name}
                    '''
    cursor.execute(select_query)
    records = cursor.fetchall()
    return {t.strftime('%Y-%m-%d %H:%M:%S'): float(v) for t, v in records}


@app.delete("/delete/{currency}")
async def delete_table(currency: str) -> Dict[str, bool]:
    """Delete table of selected currency.

    Args:
        currency (str): Shortcut of selected currency.

    Raises:
        Exception: Table does not exist.
        HTTPException: ccxt.BaseError.

    Returns:
        Dict[str, bool]: {"Table deleted succesfully": True}.
    """
    currency_symbol = f"{currency.upper()}/USDT"
    table_name = currency_symbol.replace("/", "_").lower()
    connection = None
    cursor = None
    try:
        connection = connect()
        cursor = connection.cursor()
        if not table_exist(table_name, cursor):
            raise Exception(f"Table {table_name} does not exist.")
        delete_table_from_db(table_name, cursor, connection)
    except ccxt.BaseError as e:
        raise HTTPException(status_code=400, detail=f"An error occurred with KuCoin API: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
    return {"Table deleted successfully": True}


def delete_table_from_db(table_name: str,
                         cursor: psycopg2.extensions.cursor,
                         connection: psycopg2.extensions.connection) -> None:
    """Delete table from db by query.

    Args:
        table_name (str): Name of table to delete.
        cursor (psycopg2.extensions.cursor): Cursor to selected database.
        connection (psycopg2.extensions.connection): Connection to selected database.
    """
    drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
    cursor.execute(drop_table_query)
    connection.commit()
    print(f"Table '{table_name}' deleted successfully.")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
