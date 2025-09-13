# Used Car Data Pipeline

This repository contains an Apache Airflow-based ETL pipeline for collecting, processing, and analyzing used car data from multiple sources. The pipeline consists of three main DAGs that work together to scrape, store, and transform data for analytics purposes.

## Architecture Overview

The pipeline follows a three-stage architecture:

1. **Raw Data Collection** (`raw_car_dag.py`) - Scrapes used car listings from Cars.com
2. **Dealer Data Collection** (`dealer_dag.py`) - Scrapes dealer information and reviews from DealerRater.com
3. **Data Processing** (`cleand_car_dag.py`) - Transforms and cleans the collected data using Apache Spark

## DAG Descriptions

### 1. Raw Car Data DAG (`insert_raw_car_dag`)
- **Schedule**: Daily (`@daily`)
- **Purpose**: Scrapes used car listings from Cars.com for the Seattle area (ZIP: 98104)
- **Data Collected**: Car name, dealer, price, mileage, and listing URL
- **Target Table**: `dbo.raw_car_data`

**Key Features:**
- Scrapes up to 50 listings per run
- Uses MERGE statements to handle duplicate entries
- Implements retry logic with exponential backoff
- Automatically triggers the cleaning DAG upon completion

### 2. Dealer Data DAG (`insert_dealer_dag`)
- **Schedule**: Weekly (`@weekly`)
- **Purpose**: Collects dealer information and reviews from DealerRater.com
- **Data Collected**: Dealer name, description, location, rating score, amenities, and profile URL
- **Target Table**: `dbo.dealers`

**Key Features:**
- Focuses on used car dealers in Seattle area (98109)
- Stores amenities as JSON data
- Implements rate limiting to respect website policies
- Includes error handling for failed scraping attempts

### 3. Clean Car Data DAG (`clean_car_dag`)
- **Schedule**: Daily (`@daily`)
- **Purpose**: Processes raw car data using Apache Spark for analytics
- **Data Source**: `dbo.used_cars` table
- **Processing**: ETL operations using Spark for data transformation and cleaning

**Key Features:**
- Integrates with Apache Spark for large-scale data processing
- Secure connection handling through Airflow connections
- Configurable Spark settings for optimal performance

## Prerequisites

### Software Requirements
- Apache Airflow 2.x
- Apache Spark (configured with Airflow)
- Microsoft SQL Server
- Python 3.8+

### Python Dependencies
```bash
pip install apache-airflow
pip install apache-airflow-providers-apache-spark
pip install apache-airflow-providers-microsoft-mssql
pip install requests
pip install beautifulsoup4
pip install fake-useragent
pip install pendulum
```

### Additional Requirements
- MS SQL JDBC Driver: `mssql-jdbc-12.2.0.jre8.jar` (should be placed in `/opt/spark/jars/`)
- Spark application script: `./include/scripts/mssql_to_spark.py`

## Configuration

### 1. Airflow Connections

Create the following connections in Airflow:

**Database Connection (`car_db`)**
- Connection Type: Microsoft SQL Server
- Host: Your SQL Server host
- Database: Your database name
- Login: Database username
- Password: Database password
- Port: 1433 (or your custom port)

**Spark Connection (`car_db_spark`)**
- Connection Type: Spark
- Host: Your Spark master URL
- Extra: Additional Spark configuration as JSON

### 2. Database Schema

Ensure the following tables exist in your SQL Server database:

```sql
-- Raw car data table
CREATE TABLE dbo.raw_car_data (
    car_name NVARCHAR(255),
    dealer NVARCHAR(255),
    price INT,
    mileage INT,
    car_link NVARCHAR(500) PRIMARY KEY
);

-- Dealers table
CREATE TABLE dbo.dealers (
    title NVARCHAR(255),
    description NTEXT,
    location NVARCHAR(255),
    score FLOAT,
    amenities NTEXT, -- JSON data
    link NVARCHAR(500) PRIMARY KEY
);

-- Used cars table (for processed data)
CREATE TABLE dbo.used_cars (
    -- Define columns based on your Spark processing requirements
);
```

### 3. Directory Structure
```
project/
├── dags/
│   ├── raw_car_dag.py
│   ├── dealer_dag.py
│   └── cleand_car_dag.py
└── include/
    └── scripts/
        └── mssql_to_spark.py
```

## Usage

1. **Deploy the DAGs** to your Airflow DAGs folder
2. **Configure connections** in the Airflow UI
3. **Enable the DAGs** in the Airflow interface
4. **Monitor execution** through the Airflow web interface

### Manual Triggering
You can manually trigger any DAG through the Airflow UI or using the CLI:
```bash
airflow dags trigger insert_raw_car_dag
airflow dags trigger insert_dealer_dag
airflow dags trigger clean_car_dag
```

## Data Flow

```
Cars.com → Raw Car DAG → SQL Server (raw_car_data)
                           ↓
                    Clean Car DAG → Spark Processing → SQL Server (used_cars)

DealerRater.com → Dealer DAG → SQL Server (dealers)
```

## Monitoring and Maintenance

### Key Metrics to Monitor
- DAG success/failure rates
- Scraping success rates
- Data quality metrics
- Processing times

### Common Issues
- **Website blocking**: Implement proper rate limiting and user agent rotation
- **Network timeouts**: Adjust retry settings and delays
- **Data quality**: Monitor for missing or malformed data
- **Database locks**: Ensure proper transaction handling

## Best Practices

1. **Rate Limiting**: Respect website rate limits to avoid being blocked
2. **Error Handling**: Implement comprehensive error handling and retry logic
3. **Data Validation**: Validate scraped data before insertion
4. **Monitoring**: Set up alerts for failed DAG runs
5. **Resource Management**: Monitor Spark resource usage and adjust configurations

## Security Considerations

- Store database credentials securely using Airflow Connections
- Use environment variables for sensitive configuration
- Implement proper access controls on the database
- Regular security updates for all dependencies

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests where appropriate
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
1. Check the Airflow logs for detailed error messages
2. Verify database connectivity and permissions
3. Ensure all required dependencies are installed
4. Review the DAG configuration and connections

---

**Note**: This pipeline is designed for educational and research purposes. Always ensure compliance with website terms of service and applicable data protection regulations when scraping data.
