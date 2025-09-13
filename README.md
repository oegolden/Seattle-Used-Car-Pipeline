# Used Car Data Pipeline

This repository contains an Apache Airflow-based ETL pipeline for collecting, processing, and analyzing used car data from multiple sources. The pipeline is deployed and running on Astronomer, providing automated data collection and processing for used car market analysis.

## Project Overview

The pipeline follows a three-stage architecture designed to collect comprehensive used car market data from the Seattle area. It combines listing data from Cars.com with dealer information from DealerRater.com, then processes the data through Apache Spark for analytics-ready output.

### Business Value
- **Market Intelligence**: Tracks used car pricing trends and inventory levels
- **Dealer Analysis**: Monitors dealer performance, ratings, and service offerings
- **Data-Driven Insights**: Provides clean, structured data for business intelligence and reporting
- **Automated Collection**: Reduces manual data gathering efforts with scheduled automation

## Architecture Overview

The pipeline consists of three interconnected DAGs that work together to create a comprehensive used car data warehouse:

1. **Raw Data Collection** - Captures current used car listings
2. **Dealer Intelligence** - Gathers dealer profiles and customer feedback
3. **Data Processing** - Transforms raw data into analytics-ready format

## Data Sources and Collection

### Cars.com Integration
- **Target Market**: Seattle area (ZIP: 98104)
- **Data Scope**: Up to 50 used car listings per daily run
- **Information Captured**: Vehicle details, pricing, mileage, dealer information, and listing URLs
- **Update Frequency**: Daily collection ensures current market data

### DealerRater.com Integration  
- **Coverage Area**: Seattle region (98109)
- **Focus**: Used car dealerships with customer ratings ≥1.0
- **Data Points**: Dealer names, descriptions, locations, customer ratings, amenities, and profile links
- **Update Frequency**: Weekly collection captures dealer profile changes

## DAG Architecture

### Raw Car Data Collection (`insert_raw_car_dag`)
**Purpose**: Primary data collection from Cars.com for current market listings

**Key Features:**
- Daily execution schedule aligned with market activity
- Intelligent duplicate handling using MERGE operations
- Automatic triggering of downstream processing
- Robust error handling with retry mechanisms

**Data Output**: Populated `raw_car_data` table with current inventory

### Dealer Data Collection (`insert_dealer_dag`)
**Purpose**: Comprehensive dealer profile and reputation data gathering

**Key Features:**
- Weekly scheduling optimized for dealer profile update frequency
- JSON storage for flexible amenity data
- Rate-limited scraping to maintain website relationships
- Comprehensive error logging for data quality assurance

**Data Output**: Populated `dealers` table with current dealer information

### Data Processing Pipeline (`clean_car_dag`)
**Purpose**: Transform raw collected data into analytics-ready format using Apache Spark

**Key Features:**
- Spark-based processing for scalable data transformation
- Secure connection management through Airflow
- Daily processing ensures fresh analytical data
- Optimized for large-scale data operations

**Data Output**: Processed `used_cars` table ready for business intelligence tools

## Data Flow and Dependencies

```
Daily: Cars.com → Raw Car Collection → Spark Processing → Analytics Tables
Weekly: DealerRater.com → Dealer Collection → Dealer Intelligence Tables
```

The raw car collection automatically triggers the cleaning process, ensuring that newly collected data is immediately processed and available for analysis.

## Technical Implementation

### Data Storage Strategy
- **Microsoft SQL Server**: Central data warehouse for all collected information
- **Incremental Loading**: Efficient processing of only new and updated records
- **Data Integrity**: MERGE operations prevent duplicates while capturing updates

### Processing Architecture  
- **Apache Spark Integration**: Handles large-scale data transformations
- **Airflow Orchestration**: Manages complex dependencies and retry logic
- **Astronomer Platform**: Provides enterprise-grade pipeline management and monitoring

### Data Quality Measures
- **Validation**: Input data validation before database insertion
- **Error Handling**: Comprehensive logging and retry mechanisms
- **Monitoring**: Built-in pipeline health monitoring through Astronomer

## Project Structure

The pipeline is organized into three main components:
- **Collection DAGs**: Handle web scraping and initial data capture
- **Processing DAGs**: Transform and clean collected data
- **Support Scripts**: Spark applications for data processing operations

## Data Applications

The collected and processed data supports various analytical use cases:

- **Price Analysis**: Track pricing trends across different vehicle types and dealers
- **Market Inventory**: Monitor available inventory levels and turnover rates  
- **Dealer Performance**: Analyze dealer ratings, customer satisfaction, and service quality
- **Market Intelligence**: Generate insights on market dynamics and opportunities

## Pipeline Benefits

**Automation**: Eliminates manual data collection efforts while ensuring data freshness
**Scalability**: Spark-based processing handles growing data volumes efficiently  
**Reliability**: Astronomer platform provides enterprise-grade uptime and monitoring
**Flexibility**: Modular design allows easy extension to additional data sources
**Data Quality**: Built-in validation and error handling ensures reliable data output

---

This pipeline represents a comprehensive solution for used car market data intelligence, combining automated data collection, robust processing, and enterprise-grade deployment on the Astronomer platform.
