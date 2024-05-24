import requests 
import json
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text  # Make sure to import text
import psycopg2
from config import config
import matplotlib.pyplot as plt
import os
import csv

class DataProcessor:
    def __init__(self):
        json_user = "files/user_data_json.json"
        json_wthr= "files/weather_data_json.json"
        csv_user= "files/user_data_csv.csv"
        csv_wthr= "files/weather_data_csv.csv"
        sales_csv="files/sales_data.csv"
        current_directory = os.getcwd()
        self.local_path_user_json = os.path.join(current_directory,json_user)
        self.local_path_wthr_json = os.path.join(current_directory,json_wthr)
        self.local_path_user_csv = os.path.join(current_directory,csv_user)
        self.sales_csv = os.path.join(current_directory,sales_csv)
        self.local_path_wthr_csv = os.path.join(current_directory,csv_wthr)

    
    def extract_data(self):
        raw_data = requests.get('https://jsonplaceholder.typicode.com/users')
        raw_data.raise_for_status() 
        data = raw_data.json()
        with open(self.local_path_user_json, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        print(f"Data downloaded and saved to {self.local_path_user_json}")

        raw_data.raise_for_status() 
        extracted_data = []
        for entry in data:
            extracted_entry = {
            'id': entry['id'],
            'name': entry['name'],
            'email': entry['email'],
            'street': entry['address']['street'],
            'suite': entry['address']['suite'],
            'city': entry['address']['city'],
            'zipcode': entry['address']['zipcode'],
            'lat': entry['address']['geo']['lat'],
            'lng': entry['address']['geo']['lng'],
            'phone': entry['phone'],
            'website': entry['website'],
            'company_name': entry['company']['name'],
            'company_catchphrase': entry['company']['catchPhrase'],
            'business': entry['company']['bs']
             }
            extracted_data.append(extracted_entry)

        df = pd.DataFrame(extracted_data)
        df.to_csv(self.local_path_user_csv, index=False)
        print(f"Data extracted and saved to {self.local_path_user_csv}")
    
################################
        api_key = 'b5c0b5a16289dd790c2ea0606f38554a'
        weather_data = []
        df = pd.read_csv(self.sales_csv)
        for index, row in df.iterrows():
            lat = row['lat']
            lon = row['long']
            user_id = row['customer_id']  # Get the user id from the DataFrame

            url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
             # Make the API call
            response = requests.get(url)
            if response.status_code == 200:
                weather_info = response.json()
                weather_info['user_id'] = user_id  # Append the user id to the weather_info

                weather_data.append(weather_info)
            else:
                print(f"Failed to get weather data for {lat}, {lon}")

        with open(self.local_path_wthr_json, 'w', encoding='utf-8') as file:
            json.dump(weather_data, file, ensure_ascii=False, indent=4)
        print(f"Data downloaded and saved to {self.local_path_wthr_json}")
        extracted_data_Wtr = []
        for items in weather_data:
            extracted_entry = {
            'user_id': items['user_id'],
            'lat': items['coord']['lat'],
            'lon': items['coord']['lon'],
            'weather_id': items['weather'][0]['id'],
            'weather_main': items['weather'][0]['main'],
            'weather_description': items['weather'][0]['description'],
            'weather_icon': items['weather'][0]['icon'],
            'base': items['base'],
            'temperature': items['main']['temp'],
            'feels_like': items['main']['feels_like'],
            'temp_min': items['main']['temp_min'],
            'temp_max': items['main']['temp_max'],
            'pressure': items['main']['pressure'],
            'humidity': items['main']['humidity'],
            'sea_level': items['main'].get('sea_level', None),
            'grnd_level':items['main'].get('grnd_level', None),
            'visibility': items.get('visibility',None),
            'wind': items['wind'].get('speed',None),
            'wind_deg': items['wind'].get('deg',None),
            'wind_gust': items['wind'].get('gust',None),
            'rain': items.get('rain', None),
            'clouds': items.get('clouds', None),
            'dt': items['dt'],
            'sys': items['sys'].get('sunrise',None),
            'sys': items['sys'].get('sunset',None),
            'timezone': items['timezone'],
            'id': items['id'],
            'name': items['name'],
            'cod': items['cod']
            }
            extracted_data_Wtr.append(extracted_entry)
        df_wthr = pd.DataFrame(extracted_data_Wtr)

        df_wthr.to_csv(self.local_path_wthr_csv, index=False)
        print(f"Data extracted and saved to {self.local_path_wthr_csv}")

    def load_stg_table(self):
        params = config()
        username = params['user']
        password = params['password']
        host = params['host']
        port = params['port']
        database = params['database']
        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')
        df = pd.read_csv(self.sales_csv)
        df.to_sql('stg_sales', con=engine, index=False, if_exists='replace')  # Use 'append' if you want to add to an existing table

        with engine.connect() as connection:
            result = connection.execute(text("SELECT * FROM stg_sales LIMIT 5;"))
            for row in result:
                print(row)

        df = pd.read_csv(self.local_path_user_csv)
        df.to_sql('stg_users', con=engine, index=False, if_exists='replace')  
        with engine.connect() as connection:
            result = connection.execute(text("SELECT * FROM stg_users LIMIT 5;"))
            for row in result:
                print(row)# Use 'append' if you want to add to an existing table
    
        df = pd.read_csv(self.local_path_wthr_csv)
        df.to_sql('stg_weather', con=engine, index=False, if_exists='replace')  
        with engine.connect() as connection:
            result = connection.execute(text("SELECT * FROM stg_weather LIMIT 5;"))
            for row in result:
                print(row)# Use

    def transform(self):
        """ Create tables in the PostgreSQL database"""
        sql = (
        
        """INSERT INTO d_users
            select id,name,email,street,suite,city,zipcode,lat as latitude,lng as longitude
            from stg_users
        """,
        """INSERT INTO d_weather
            select user_id,lat,lon,weather_main,weather_description,temperature,feels_like,
            temp_min,temp_max,pressure,humidity,wind
            
            from stg_weather
        """,
        """INSERT INTO fct_Sales
            (ORDER_ID,USER_ID,product_id,quantity,price,DATE_ID,WEATHER_ID)
            select distinct ORDER_ID,customer_id AS USER_ID,product_id,quantity,price,
            d.date_id AS DATE_ID,wthr.WEATHER_ID
            from stg_sales stg  join d_Date d on stg.order_Date::date=d.date
            join d_weather wthr on stg.customer_id::VARCHAR=wthr.USER_ID::VARCHAR 
            and stg.LAT=wthr.LATITUDE
            AND stg.LONG=wthr.LONGITUDE
        """
        )
   
        try:
            params = config()
            with psycopg2.connect(**params) as conn:
                with conn.cursor() as cur:
                    for command in sql:
                        cur.execute(command)

        except (psycopg2.DatabaseError, Exception) as error:
            print(error)


    def aggregate_table_transform(self):
    
        sql = (
        """
         DROP TABLE IF EXISTS TOT_SALES_PER_CUSTOMER;

         CREATE TABLE TOT_SALES_PER_CUSTOMER  AS
         (SELECT user_id,NAME,TOTAL_PRICE,
         RANK() OVER (PARTITION BY user_id ORDER by TOTAL_PRICE DESC) AS CUSTOMER_PERF FROM 
         (SELECT users.user_id,NAME,SUM(PRICE) AS TOTAL_PRICE
          FROM FCT_SALES SALES, D_USERS USERS WHERE SALES.user_id=USERS.user_id
          GROUP BY users.user_id,NAME
         )T)
        """,
        """
         DROP TABLE IF EXISTS AVG_ORDER_QTY;

         CREATE TABLE AVG_ORDER_QTY  AS
         (SELECT product_id,AVG_QUANTITY,
         RANK() OVER (PARTITION BY product_id ORDER by AVG_QUANTITY DESC) AS PRODUCT_PERF FROM 

         (SELECT product_id,AVG(quantity) AS AVG_QUANTITY
          FROM FCT_SALES SALES
          GROUP BY product_id
         )T)
        """,
        """
         DROP TABLE IF EXISTS SUM_SALES_PER_WEATHER;

         CREATE TABLE sum_SALES_PER_WEATHER  AS
         (SELECT 
               weather,
            sum(price) AS SUM_sales_amount
            FROM 
          FCT_SALES sales
            JOIN 
                    D_WEATHER weather ON sales.WEATHER_ID = weather.WEATHER_ID
            GROUP BY 
            weather)
        """,
        """
        DROP TABLE IF EXISTS SALES_BY_USER;
        CREATE TABLE SALES_BY_USER  AS
        (SELECT u.user_id, u.name,u.email,u.street,u.city,u.COMPANY_NAME,u.COMPANY_BUSINESS,
          s.order_id, s.product_id, s.quantity, s.price
            FROM FCT_SALES s    
            JOIN D_USERS u ON s.user_id = u.user_id
        )
        """,
        """
        DROP TABLE IF EXISTS SALES_BY_WEATHER;
        CREATE TABLE SALES_BY_WEATHER  AS
        (SELECT s.order_id, s.product_id, s.quantity, s.price, w.weather,w.weather_desc
        FROM FCT_SALES s JOIN D_WEATHER w ON s.weather_id = w.weather_id)
        """,
        """
        DROP TABLE IF EXISTS SALES_BY_DATE;
        CREATE TABLE SALES_BY_DATE  AS
        (SELECT d.date, SUM(s.price) AS total_sales
        FROM FCT_SALES s JOIN D_DATE d ON s.date_id = d.date_id
        GROUP BY d.date
        ORDER BY d.date
        )
        """
        )
        try:
            params = config()
            with psycopg2.connect(**params) as conn:
                with conn.cursor() as cur:
                    for command in sql:
                        cur.execute(command)

        except (psycopg2.DatabaseError, Exception) as error:
            print(error)
    def visualize(self):
        params = config()
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                cur = conn.cursor()
                cur.execute("SELECT user_id,TOTAL_PRICE FROM TOT_SALES_PER_CUSTOMER ORDER BY TOTAL_PRICE")
                data = cur.fetchall()
                user_id = [row[0] for row in data]
                TOTAL_PRICE = [row[1] for row in data]
                fig, axs = plt.subplots(3)
                axs[0].bar(user_id, TOTAL_PRICE, color='blue')
                axs[0].set_title('Total Sales per Customer')
                axs[0].legend()
                
                cur.execute("SELECT year , SUM(price) AS total_sales_amount FROM FCT_SALES s, D_DATE d where s.date_id=s.date_id  GROUP by year")
                data = cur.fetchall()
                year = [(row[0]) for row in data]
                total_sales_amount = [row[1] for row in data]
                axs[1].bar(year, total_sales_amount, color='blue')
                axs[1].set_title('Total Sales per year')
                axs[1].legend()
                axs[1].set_ylim(0, max(total_sales_amount) * 0.8)  # Decrease the range by 20%
               
                plt.tight_layout()
                plt.savefig('Sales_Analysis.png')             
                
                cur.execute("SELECT weather,SUM_sales_amount FROM sum_SALES_PER_WEATHER ORDER BY SUM_sales_amount")
                data = cur.fetchall()
                weather = [row[0] for row in data]
                SUM_sales_amount = [int(row[1]) for row in data]
                axs[2].plot(weather,SUM_sales_amount, color='blue')
                axs[2].set_title('Average Sales per weather')
                axs[2].legend()
                axs[2].set_ylim(0, max(SUM_sales_amount) * 0.8)
                             
                plt.tight_layout()
                plt.savefig('Sales_Analysis.png')
               
# Close the cursor and connection
            cur.close()
        conn.close()

processor = DataProcessor()
processor.extract_data()
processor.load_stg_table()
processor.transform()
processor.aggregate_table_transform()
processor.visualize()