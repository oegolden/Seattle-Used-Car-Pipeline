from airflow.sdk.definitions.asset import Asset
from airflow.decorators import dag, task
from pendulum import datetime

import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col,isnan, when, count,median
import os
import sys
import math
#Mileage regression model imports
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from pyspark.sql.functions import col, when, concat_ws
from pyspark.sql.types import StringType, IntegerType
import requests
from bs4 import BeautifulSoup
import time
from fake_useragent import UserAgent
import multiprocessing as mp
import pyodbc
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

BASE_URL = "https://www.cars.com"
HEADERS = { "Accept": "application/json, text/plain, */*", # change this to your need
    "Accept-Encoding": "gzip, deflate, br, zstd", # this as well
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8", # can be ignored
    "Connection": "keep-alive", # usually kept 
    "Sec-Fetch-Dest": "empty", # security add-on, can be ignored
    "Sec-Fetch-Mode": "cors", # security add-on, can be ignored
    "Sec-Fetch-Site":"same-site",  # security add-on, can be ignored
    'User-Agent':ua.random}

@dag(
    start_date=datetime(2025, 8, 20),
    schedule_interval='@daily',
    tags=['used_car'],
    catchup=False,
)
def insert_car_data():
    """
    DAG to insert used car data into a database.
    This DAG runs daily and is designed to handle the insertion of used car data.
    """

    # @task
    # def get_total_pages() -> int:
    #     """
    #     This task retrieves the total number of pages of used car data from an API.
    #     It pushes the total number of pages to XCom for use in subsequent tasks.
    #     """
    #     ua = UserAgent()
    #     # Loop through search result pages
    #     url = f"{BASE_URL}/shopping/results/?include_shippable=true&maximum_distance=10&page=1&page_size=50&seller_type[]=dealership&sort=best_match_desc&stock_type=used&zip=98104"
    #     res = requests.get(url, headers=HEADERS)
    #     soup = BeautifulSoup(res.text, 'html.parser')
    #     num = soup.find("span", class_= "total-filter-count").get_text(strip=True)
    #     num = int(''.join(c for c in num if c.isdigit()))
    #     return num
    
    def year_brand_columns(df):
        car_brand_udf = udf(car_brand)
        car_year_udf = udf(car_year)
        return df.withColumn("car_brand", car_brand_udf(df.car_name)).withColumn("car_year", car_year_udf(df.car_name))
    
    def null_count(df):
        return df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])

    def car_brand(x):
        if x != None:
            words = x.split(' ')
            return words[1]
        else:
            return x

    def car_year(x):
        if x != None:
            words = x.split(' ')
            return int(words[0])
        else:
            return x

    def fillna_median(df, include=set()): 
        medians = df.agg(*(
            median(x).alias(x) for x in df.columns if x in include
        ))
        return df.fillna(medians.first().asDict())
    

    def scrape_page():
        ua = UserAgent()

        car_dict = {}

        # Loop through search result pages
        try:
            print(f"Scraping listing page {page_num}")
            url = f"{BASE_URL}/shopping/results/?include_shippable=true&maximum_distance=10&page={page_num}&page_size=50&seller_type[]=dealership&sort=sort=listed_at_desc&stock_type=used&zip=98104"
            res = requests.get(url, headers=HEADERS)
            soup = BeautifulSoup(res.text, 'html.parser')
        except:
            
            print(f"Error scraping {link}: {e}")
            time.sleep(0.5)
            rows = []
            car_dict
            for key,value in car_dict.items():
                rows.append(value + [key])
            return rows


        # Find all vehicle cards
        cards = soup.find_all("a", class_="vehicle-card-link")
        links = [BASE_URL + card.get("href") for card in cards]
        time.sleep(1)  # Be nice to the server

        # Now for all the links on that page, scrape each link
        for i, link in enumerate(links):
            print(f"[{i+1}/{len(links)}] Scraping {link}")
            try:
                res = requests.get(link, headers=HEADERS)
                soup = BeautifulSoup(res.text, 'html.parser')

                title = soup.find("h1", class_="listing-title").get_text(strip=True)
                price = soup.find("span", class_="primary-price").get_text(strip=True)
                mileage = soup.find("p", class_="listing-mileage").get_text(strip=True)
                dealer = soup.find("h3", class_="spark-heading-5 heading seller-name").get_text(strip=True)
                price = int(''.join(c for c in price if c.isdigit()))
                mileage = int(''.join(c for c in mileage if c.isdigit()))
                car_dict[link] = [title,dealer,price,mileage]
            except Exception as e:
                print(f"Error scraping {link}: {e}")
            time.sleep(0.5)
        rows = []
        car_dict
        for key,value in car_dict.items():
            rows.append(value + [key])
        return rows
    
    @task.pyspark(conn_id = "my_spark_conn")
    def load_data_into_frame(spark: SparkSession,sc: SparkContext):
        """
        This task retrieves used car data from a web API and returns it as a list of dictionaries.
        """
        results = map(scrape_page, range(1,10))
        results = [item for sublist in results for item in sublist]       
        df = spark.createDataFrame(results, schema='car_name string, dealer string, price int, mileage int,car_link string')
        df = year_brand_columns(df)
        df = df.filter(df.car_link.isNotNull())
        print("DF DONE")
        data=[]  
        # collect data from the  dataframe 
        for i in df.collect(): 
            data.append(tuple(i)) 
        return data
    
    @task
    def insert_data_to_db(data):
        connectionString = f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{server};Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

        SQL_QUERY = f"""
        MERGE INTO dbo.used_cars AS target
        USING (SELECT ? AS car_name, ? AS dealer, ? AS price, ? AS mileage, ? AS car_link, ? AS car_brand, ? AS car_year) AS source
        ON target.car_link = source.car_link
        WHEN MATCHED THEN 
            UPDATE SET 
                car_name = source.car_name,
                dealer = source.dealer,
                price = source.price,
                mileage = source.mileage,
                car_brand = source.car_brand,
                car_year = source.car_year
        WHEN NOT MATCHED THEN 
            INSERT (car_name, dealer, price, mileage, car_link, car_brand, car_year)
            VALUES (source.car_name, source.dealer, source.price, source.mileage, source.car_link, source.car_brand, source.car_year);
        """
        print("inserting data")
        conn = pyodbc.connect(connectionString,timeout=30)
        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.executemany(SQL_QUERY, data)

        conn.commit()
        conn.close()

    # Define the DAG tasks
    data = load_data_into_frame()
    insert_data_to_db(data)

insert_car_data()