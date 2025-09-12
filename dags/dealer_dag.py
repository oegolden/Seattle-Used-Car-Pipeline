from airflow.decorators import dag, task
from pendulum import datetime
from datetime import timedelta
from airflow.sdk.definitions.asset import Asset
import requests
from bs4 import BeautifulSoup
import time
from fake_useragent import UserAgent
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import json
BASE_URL = "https://www.cars.com"
ua = UserAgent()
HEADERS = { 
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site":"same-site",
    'User-Agent': ua.random
}
@dag(
    dag_id="insert_dealer_dag",
    start_date=datetime(2025, 9, 5),
    schedule='@weekly',
    tags=['used_car'],
    catchup=False,
)
def insert_dealer_data():
    """
    DAG to insert used car dealer data into a database.
    This DAG runs daily and is designed to handle the insertion of used car dealer data.
    """
    
    @task
    def scrape_all_reviews():
        """Scrape one page of car listings (simplified)."""
        url = "https://www.dealerrater.com/directory/z/98109/Used-Car%20Dealer/25/?typedCriteria=98125&MinRating=1"
        res = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(res.text, 'html.parser')

        # Find all vehicle cards
        cards = soup.find_all("a", class_="teal dealer-name-link")
        links = ["https://www.dealerrater.com/" + card.get("href") for card in cards]
        time.sleep(1)
        # Scrape each listing
        dealer_dict = {}
        for i, link in enumerate(links):
            try:
                res = requests.get(link, headers=HEADERS)
                soup = BeautifulSoup(res.text, 'html.parser')
                title = soup.find("p", class_="boldest font-24 margin-bottom-none").get_text(strip=True)
                description = soup.find("span", class_="font-18 margin-bottom-none block").get_text(strip=True)
                location = soup.find("span", class_="black font-16 notranslate").get_text(strip=True)
                score = soup.find("span", class_="font-36 bolder rating-number line-height-1").get_text(strip=True)
                ameneties = []
                ameneties = soup.find_all("li", class_="line-height-1 pad-bottom-sm")
                ameneties = [amenity.get_text(strip=True) for amenity in ameneties]
                dealer_dict[link] = [title, description, location, score, ameneties,link]
            except Exception as e:
                print(f"Error scraping {link}: {e}")
            time.sleep(0.5)
        return dealer_dict
    
    @task
    def transform_dealer_data(dealer_dict):
        for key in dealer_dict.keys():
            dealer_dict[key][4] = json.dumps(dealer_dict[key][4])
            dealer_dict[key][3] = float(dealer_dict[key][3])
        return [tuple(value) for value in dealer_dict.values()]
    
    @task(retries=3, retry_delay=timedelta(minutes=2))
    def insert_data_to_db(data):
        hook = MsSqlHook(mssql_conn_id="car_db")
        merge_sql = """
        INSERT INTO dbo.dealers (title, description, location, score, amenities, link)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        for row in data:   # one MERGE per row
            hook.run(merge_sql, parameters=row)
            
    insert_data_to_db(transform_dealer_data(scrape_all_reviews()))
insert_dealer_data()