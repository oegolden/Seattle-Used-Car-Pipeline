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
    dag_id="insert_raw_car_dag",
    start_date=datetime(2025, 9, 5),
    schedule='@daily',
    tags=['used_car'],
    catchup=False,
)
def insert_car_data():
    """
    DAG to insert used car data into a database.
    This DAG runs daily and is designed to handle the insertion of used car data.
    """
    
    @task
    def scrape_page():
        """Scrape one page of car listings (simplified)."""
        car_dict = {}
        url = f"{BASE_URL}/shopping/results/?include_shippable=true&maximum_distance=10&page=1&page_size=50&seller_type[]=dealership&sort=listed_at_desc&stock_type=used&zip=98104"
        res = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(res.text, 'html.parser')

        # Find all vehicle cards
        cards = soup.find_all("a", class_="vehicle-card-link")
        links = [BASE_URL + card.get("href") for card in cards]
        time.sleep(1)

        # Scrape each listing
        for i, link in enumerate(links):
            try:
                res = requests.get(link, headers=HEADERS)
                soup = BeautifulSoup(res.text, 'html.parser')
                title = soup.find("h1", class_="listing-title").get_text(strip=True)
                price = soup.find("span", class_="primary-price").get_text(strip=True)
                mileage = soup.find("p", class_="listing-mileage").get_text(strip=True)
                dealer = soup.find("h3", class_="spark-heading-5 heading seller-name").get_text(strip=True)
                price = int(''.join(c for c in price if c.isdigit()))
                mileage = int(''.join(c for c in mileage if c.isdigit()))
                car_dict[link] = [title, dealer, price, mileage, link]
            except Exception as e:
                print(f"Error scraping {link}: {e}")
            time.sleep(0.5)

        # Convert to list of tuples for executemany-style
        print(car_dict)
        return [tuple(value) for value in car_dict.values()]

    # MsSqlOperator — parameterized insert/update

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def insert_data_to_db(data):
        hook = MsSqlHook(mssql_conn_id="car_db")
        merge_sql = """
        MERGE INTO dbo.raw_car_data AS target
        USING (SELECT %s AS car_name, %s AS dealer, %s AS price, %s AS mileage, %s AS car_link) AS source
        ON target.car_link = source.car_link
        WHEN MATCHED THEN UPDATE SET
        car_name = source.car_name,
        dealer   = source.dealer,
        price    = source.price,
        mileage  = source.mileage
        WHEN NOT MATCHED THEN
        INSERT (car_name, dealer, price, mileage, car_link)
        VALUES (source.car_name, source.dealer, source.price, source.mileage, source.car_link);
        """
        for row in data:   # one MERGE per row
            hook.run(merge_sql, parameters=row)

    
    trigger_second = TriggerDagRunOperator(
        task_id="trigger_clean_car_dag",
        trigger_dag_id="clean_car_dag",  # downstream DAG id
        wait_for_completion=False,    # set True if you want first_dag to wait
    )
    
    data = scrape_page()
    insert_data_to_db(data) >> trigger_second

insert_car_data()