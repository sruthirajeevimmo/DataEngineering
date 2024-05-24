from sqlalchemy import create_engine
from sqlalchemy import text  # Make sure to import text
import json
import pandas as pd
import psycopg2
from config import config

def create_tables():
   """ Create tables in the PostgreSQL database"""
   commands = (
      
      """ 
        
        DROP TABLE IF EXISTS FCT_SALES;
        DROP TABLE IF EXISTS D_USERS;
        DROP TABLE IF EXISTS D_WEATHER;
        DROP TABLE  IF EXISTS D_DATE;
        CREATE TABLE D_USERS
        (
            USER_ID VARCHAR(20) NOT NULL,
            NAME VARCHAR(100) NOT NULL,
            EMAIL VARCHAR(100) ,
            STREET VARCHAR(100) ,
            SUITE VARCHAR(100) ,
            CITY VARCHAR(100) ,
            ZIPCODE VARCHAR(100) ,
            LATITUDE DOUBLE PRECISION ,
            LONGITUDE DOUBLE PRECISION ,
            COMPANY_NAME VARCHAR(100) ,
            COMPANY_BUSINESS VARCHAR(100) ,
            PRIMARY KEY (USER_ID)
        );
      """,
      """
        CREATE TABLE D_WEATHER
        (
            USER_ID VARCHAR(20) NOT NULL,
            LATITUDE DOUBLE PRECISION NOT NULL,
            LONGITUDE DOUBLE PRECISION NOT NULL,
            WEATHER VARCHAR(100) NOT NULL,
            WEATHER_DESC VARCHAR(100) ,
            TEMPERATURE FLOAT NOT NULL,
            FEELS_LIKS FLOAT NOT NULL,
            MINIMUM_TEMPERATURE FLOAT NOT NULL,
            MAXIMUM_TEMPERATURE FLOAT NOT NULL,
            PRESSURE FLOAT ,
            HUMIDITY FLOAT ,
            WIND_SPEED FLOAT ,
            WEATHER_ID SERIAL PRIMARY KEY
        );
      """,
      """
        CREATE TABLE D_DATE
        (
            DATE DATE NOT NULL,
            DAY_OF_WEEK VARCHAR(20) NOT NULL,
            MONTH VARCHAR(20) NOT NULL,
            QUARTER INT NOT NULL,
            YEAR INT NOT NULL,
            DATE_ID SERIAL PRIMARY KEY
        )
      """ ,
      """
        CREATE TABLE FCT_SALES
        (
            ORDER_ID VARCHAR(20) NOT NULL,
            PRODUCT_ID VARCHAR(20) NOT NULL,
            QUANTITY INT NOT NULL,
            PRICE INT NOT NULL,
            WEATHER_ID INT NOT NULL,
            USER_ID VARCHAR(20) NOT NULL,
            DATE_ID INT NOT NULL,
            FOREIGN KEY (WEATHER_ID) REFERENCES D_WEATHER(WEATHER_ID),
            FOREIGN KEY (USER_ID) REFERENCES D_USERS(USER_ID),
            FOREIGN KEY (DATE_ID) REFERENCES D_DATE(DATE_ID)
        );
      """,
      """
      INSERT INTO D_DATE (DATE, DAY_OF_WEEK, MONTH,QUARTER, YEAR)
        SELECT 
        date_series.full_date AS DATE,
        to_char(date_series.full_date, 'Day'),
        to_char(date_series.full_date, 'Month'),
        CEIL(EXTRACT(MONTH FROM date_series.full_date) / 3) AS quarter,
        extract(year from date_series.full_date)
        FROM (
        SELECT generate_series('2022-01-01'::date, '2024-01-01'::date, '1 day'::interval)::date AS full_date
        ) date_series;
      """
    )
   try:
        params = config()
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                # execute the CREATE TABLE statement
                for command in commands:
                    cur.execute(command)
   except (psycopg2.DatabaseError, Exception) as error:
        print(error)

if __name__ == '__main__':
    create_tables()