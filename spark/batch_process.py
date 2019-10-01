import pandas as pd
import os
import configparser
import sys
from pyspark.sql import SparkSession
import numpy as np
from pyspark import SparkContext as sc
import json
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lag


def get_username(config):
    return config.get('properties', 'username')


def get_password(config):
    return config.get('properties', 'password')


def ReadJSONAndRegBiznsTable(file_path, spark):
    """
    read business json file from HDFS
    """
    Business_data = spark.read.json(file_path)
    # explode categories for business which has multiple categories
    Business_data = Business_data.withColumn("categories",
                                             explode(split(col("categories"), "\,").cast("array<String>")))

    Business = Business_data.select(Business_data.categories, Business_data.business_id,
                                    Business_data.city, Business_data.state, Business_data.postal_code,
                                    Business_data.longitude, Business_data.latitude, Business_data.city,
                                    Business_data.stars, Business_data.review_count)
    # Register as temp table
    Business.registerTempTable("Business")
    # Run the SQL Query to calculate average star, total review etc for each category, business in each city
    result = spark.sql("SELECT lower(Business.city) as city, lower(Business.categories) as categories,lower(Business.state) as state,Business.postal_code, \
    Business.longitude,Business.latitude, AVG(Business.review_count) As AverageReview, \
    AVG(Business.stars) as AverageStars, SUM(Business.review_count) As TotalReview, \
    COUNT(DISTINCT business_id) as BusinessCount \
    FROM Business GROUP BY 1,2,3,4,5,6")
    return result


def WriteCategoryTbl(spark, table_name, rds_url, result):
    # write to postgres database
    mode = "overwrite"
    config = configparser.ConfigParser()
    config.read("/home/ubuntu/s3/myConfig.config")
    properties = {"user": get_username(config), "password": get_password(
        config), "driver": "org.postgresql.Driver"}
    result.write.jdbc(url=rds_url, table=table_name,
                      mode=mode, properties=properties)


def ReadJSONAndRegLagTable(file_path, spark):
    """
    Use Spark Session to create a table to caluclate avg check-in interval in each location
    """
    # File location and type
    file_location = file_path
    file_type = "json"

    # CSV options
    infer_schema = "true"
    first_row_is_header = "false"
    delimiter = ","

    # The applied options are for CSV files. For other file types, these will be ignored.
    df = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_path)

    checkin_data = df.withColumn("date", explode(
        split(col("date"), ",\s*").cast("array<timestamp>")))
    checkin_data.registerTempTable("checkin")
    last_event = checkin_data.withColumn("lastcheck", lag('date').over(
        Window.partitionBy('business_id').orderBy('date')))
    # avg check-in interval in hours
    lag_in_hour = last_event.withColumn(
        'lag_in_hour', (unix_timestamp('date') - unix_timestamp('lastcheck'))/150)
    lag_in_hour.registerTempTable("lag")
    result = spark.sql("SELECT lower(city) as city,lower(state) as state,lower(postal_code) as postal_code,avg(lag_in_hour) as avg_hour FROM business a join lag b \
    on a.business_id = b.business_id where state='AZ' group by 1,2,3")
    return result


def ReadRestaurantFileInTempView(csv_path, spark):
    health_df = spark.read.format("csv").options(header="true", inferSchema="true", delimiter=";")\
        .load(csv_path)
    health_df.createOrReplaceTempView("restaurant")
    return health_df


def ReadBusinessInNVToTempView(file_path, spark):
    # Business_data = spark.read.json("/FileStore/tables/business.json")
    Business_data = spark.read.json(file_path)
    business_name = Business_data.select(Business_data.name, Business_data.business_id, Business_data.city,
                                         Business_data.state, Business_data.postal_code, Business_data.longitude,
                                         Business_data.latitude, Business_data.city, Business_data.stars,
                                         Business_data.review_count, Business_data.address)
    business_name.createOrReplaceTempView("NV")
    spark.sql(
        "SELECT * from NV where lower(state) = 'nv'").createOrReplaceTempView("NV")

    # join reviews and health grade by restaurant name and address
    grade_review = spark.sql(
        "SELECT lower(a.city) as city,lower(a.state) as state,lower(a.name) as name,lower(a.postal_code) as postal_code, \
        a.longitude,a.latitude,stars,review_count,a.address, current_grade, \
         current_demerits from NV a left join restaurant b on lower(a.name) = lower(b.restaurant_name) \
          and a.postal_code=b.zip_code where current_grade is not null")
    return grade_review


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Neighbourhood analysis") \
        .getOrCreate()
    # read business json file from HDFS
    file_path = "hdfs://ec2-35-160-13-109.us-west-2.compute.amazonaws.com:9000/user/business.json"
    business_agg = ReadJSONAndRegBiznsTable(file_path, spark)

    # read check-in json file from HDFS
    file_path = "hdfs://ec2-35-160-13-109.us-west-2.compute.amazonaws.com:9000/user/checkin.json"
    # use spark session to create avg check-in interval in hours per business
    # join business location data and get avg check in interval per city per post per state
    avg_checkin = ReadJSONAndRegLagTable(file_path, spark)
    #
    #test = "hdfs://ec2-35-160-13-109.us-west-2.compute.amazonaws.com:9000/user/restaurant_establishments.csv"
    # read nightly restaurant health inspection data to table restaurant_inspection
    health_grade = ReadRestaurantFileInTempView(sys.argv[1], spark)
    # join business in NV to compare Reviews Rates and Health Inspection Rates
    file_path = "hdfs://ec2-35-160-13-109.us-west-2.compute.amazonaws.com:9000/user/business.json"
    grade_review = ReadBusinessInNVToTempView(file_path, spark)
    rds_url = "jdbc:postgresql://rds-postgresql-yelp.culiy2jimxsn.us-west-2.rds.amazonaws.com:5432/carrieliuDatabase"
    # write 4 tables to Postgres Database
    WriteCategoryTbl(spark, "public.business_agg", rds_url, business_agg)
    WriteCategoryTbl(spark, "public.avg_checkin", rds_url, avg_checkin)
    WriteCategoryTbl(spark, "public.health_grade", rds_url, health_grade)
    WriteCategoryTbl(spark, "public.grade_review", rds_url, grade_review)

    spark.stop()
