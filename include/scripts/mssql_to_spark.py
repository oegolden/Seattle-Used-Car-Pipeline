from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import col, current_date, to_date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, count, udf
from pyspark.sql.functions import expr
from pyspark.sql.functions import median


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


if __name__ == "__main__":
    jdbc_url = sys.argv[1]
    user = sys.argv[2]
    password = sys.argv[3]
    target_table = sys.argv[4]

    spark = SparkSession.builder \
        .appName("MSSQLtoSpark") \
        .getOrCreate()

    # Load data from MSSQL
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "dbo.raw_car_data") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()

    # Example transformation
    currnt_day_df = df.filter(to_date(col("ds")) == current_date())
    df_transformed = fillna_median(year_brand_columns(currnt_day_df), include={"price", "mileage"})

    # Write to external path instead of managed Hive table
    df_transformed.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", jdbc_url) \
        .option("dbtable", target_table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()


    spark.stop()
