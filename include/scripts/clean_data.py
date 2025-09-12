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


def main():
    spark = SparkSession.builder \
        .appName("used_cars") \
        .getOrCreate()
    df = spark.