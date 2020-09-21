import configparser
from TableSchema import *
from datetime import datetime
import os
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Lng, TimestampType as TST
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,  to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

i94_data = config.get('INPUT_DATA', 'I94_DATA')
airport_code_data = config.get('INPUT_DATA','AIRPORT_DATA')
us_demo_data = config.get('INPUT_DATA','US_DEMO_DATA')
world_temp_data = config.get('INPUT_DATA','WORLD_TEMP_DATA')
i94_city_name_data = config.get('INPUT_DATA','I94_CITY_NAME_DATA')



def create_spark_session():
    """Creates a spark session
       with spark.jars and apache.hadoop packages

    Parameters:No arguments

    Returns: spark session named "spark"

    """
    print("***********************************************************")
    print("Create Spark Session")
    print("***********************************************************")
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    print("***********************************************************")
    print("Spark Session Created")
    print("***********************************************************")
    print("")
    print("")
    return spark

def process_i94_data(spark, input_data):
    """import i94 immigration dataset extract columns and create persons and trip tables
    write those tables to parquet files

    Parameters:
    spark: name of spark session
    input_data: location of the source data sas
    output_data: location of the destination data

    Returns:  writes i94 table in parquet to output_data location
    writes persons_table in parquet to output_data location + persons
    writes trip_table in parquet to output_data location + trip

    """
    print(f"Processing i94 Data using {input_data}")
    df = pd.read_sas(input_data, 'sas7bdat', encoding="ISO-8859-1")
    #df = pd.read_csv("immigration_data_sample.csv", header="infer") #for testing
    print(f"data imported: {df.shape[0]} rows")
    return df


def process_csv_data(spark, input_data, delimiter, header):
    """import csv dataset extract columns and create tables
    write those tablesto parquet files

    Parameters:
    spark: name of spark session
    input_data: location of the source data sas
    output_data: location of the destination data

    Returns:  writes i94 table in parquet to output_data location
    writes persons_table in parquet to output_data location + persons
    writes trip_table in parquet to output_data location + trip

    """
    print(f"Processing CSV Data using {input_data} with a delimiter of {delimiter} and header: {header}")
    df = pd.read_csv(input_data, delimiter=delimiter, header=header)
    print(f"data imported: {df.shape[0]} rows")
    return df

def data_quality(df, tbl_name):
    print("***********************************************************")
    print(f"Cleaning Data in {tbl_name}")
    print("***********************************************************")
    rows_before = df.shape[0]
    
    clean_null_df = df.dropna(how='any',axis=0)
    rows_after = clean_null_df.shape[0]
    rows_dropped = rows_before - rows_after
    print(f"{tbl_name} rows before drop nulls: {rows_before}")
    print(f"{tbl_name} rows after drop nulls: {rows_after}")
    print(f"{tbl_name} rows dropped: {rows_dropped}")
    
    clean_sub_data = clean_null_df.drop_duplicates()
    print(f"{tbl_name} clean data contains: {clean_sub_data.shape[0]} rows")
    print("***********************************************************")
    print("")
    print("")
    return clean_sub_data


def main():
    """main function executes create_spark_session, process_song_data function, and
    process_log_data function with input_data and output_data defined.

    Parameters: No arguments
    
    Returns: No Returns


    """
    print("Main Function")
    print("Configuration Settings:")
    print(f"i94 data location: {i94_data}") 
    
   
    #create Spark Session
    spark = create_spark_session()

    i94_df = process_i94_data(spark, i94_data)
    us_demo_df = process_csv_data(spark, us_demo_data, ";", "infer")
    arp_code_df = process_csv_data(spark, airport_code_data, ";", "infer")
    world_temp_df = process_csv_data(spark, world_temp_data, ",", "infer")
   
    # create a sub i94 table with columns defined with I94_SUB_SCHEMA
    i94_sub_data = i94_df[TableSchema.I94_SUB_SCHEMA]
    
    #fill in ommitted values
    #not listed for omitted values
    i94_sub_data['occup'].fillna("not listed", inplace=True)
    
    #O for omitted values
    i94_sub_data['gender'].fillna("O", inplace=True)
    
    #create a clean sub i94 table using data_quality  
    clean_i94_data = data_quality(i94_sub_data, "i94 Dataset")
    
    #Convert datatypes
    convert_i94_sub_dict = {'admnum': int, 'biryear':int, 'i94yr': int, 'i94mon': int, 'cicid': int, 'i94mode': int}
    clean_i94_data = clean_i94_data.astype(convert_i94_sub_dict)
    clean_i94_data['depdate'] = pd.to_timedelta(clean_i94_data['depdate'], unit='d') + pd.datetime(1960, 1, 1)
    clean_i94_data['arrdate'] = pd.to_timedelta(clean_i94_data['arrdate'], unit='d') + pd.datetime(1960, 1, 1)
    
    #Create a i94 merge table
    i94_merge = i94_sub_data[TableSchema.I94_MERGE_SCHEMA]
    
    #Create a clean i94_merge table
    clean_i94_merge = data_quality(i94_merge, "i94 Merge Dataset")
    
    #Convert datatypes
    convert_i94_merge_dict = {'admnum': int, 'i94yr': int, 'i94mon': int, 'cicid': int}
    clean_i94_merge = clean_i94_merge.astype(convert_i94_merge_dict) 
        
    #create a us_demo table with columns defined with US_DEMO_SCHEMA
    us_demo_df = us_demo_df[TableSchema.US_DEMO_SCHEMA]
    us_demo_merge_df = us_demo_df[TableSchema.US_DEMO_MERGE_SCHEMA]
    
    #create a clean us demographics table
    #all columns
    clean_us_demo = data_quality(us_demo_df, "US Demographics")
    clean_us_demo["City"] = clean_us_demo["City"].str.upper()
    
    #create a clean us demographics merge table
    #all columns except Race
    clean_us_demo_merge = data_quality(us_demo_merge_df, "US Demographics Merge")
    clean_us_demo_merge["City"] = clean_us_demo_merge["City"].str.upper()
    
    #create a sub air port code table 
    airport_sub_code = arp_code_df[TableSchema.AIRPORT_SUB_SCHEMA]
    airport_sub_code['State'] = airport_sub_code['State'].str.strip()
    
    #create a clean airport code table 
    clean_airport_code = data_quality(airport_sub_code, "Airport Codes")
    
    #create a sub world temperature table set dt are datetime create two new columns wt_year and wt_month
    world_sub_temp = world_temp_df[TableSchema.WORLD_TEMP_SCHEMA]
    world_sub_temp = world_sub_temp.astype({'dt': 'datetime64[ns]'})
    world_sub_temp['wt_year'] = pd.DatetimeIndex(world_sub_temp['dt']).year
    world_sub_temp['wt_month'] = pd.DatetimeIndex(world_sub_temp['dt']).month
    
    #import date from TableSchema convert to year and month values
    wrld_temp_year = TableSchema.WORLD_TEMP_DATE.year
    wrld_temp_mon = TableSchema.WORLD_TEMP_DATE.month
    print(f"World Temperatures with be filtered by Year: {wrld_temp_year}")
    print(f"World Temperatures with be filtered by month: {wrld_temp_mon}")
    
    #create a us temperature table by date in TableSchema
    us_temp = world_sub_temp.query('Country=="United States" & wt_year==@wrld_temp_year & wt_month == @wrld_temp_mon')
    us_temp["City"] = us_temp["City"].str.upper()
    us_city_temp = pd.DataFrame(us_temp[['City', 'AverageTemperature', 'Latitude', 'Longitude']])
    
    #create a clean us temperature table
    clean_us_temp = data_quality(us_city_temp, "US Temperature")
   
    #Create location_dim table
    #Merge airport codes with us temp set Index to "Ident"
    location_dim = pd.merge(clean_us_temp, clean_airport_code, left_on='City', right_on='City', how='inner')
        
    #Create person_dim table
    person_dim = pd.DataFrame(clean_i94_data[['admnum', 'gender', 'biryear', 'visatype', 'occup']])
    
    #Create travel_dim table
    travel_dim = pd.DataFrame(clean_i94_data[['cicid', 'airline', 'fltno', 'depdate', 'arrdate', 'i94mode', 'i94addr']])
    
    #Group US demographics, unstack, clean column names, de multi index, and re combine
    us_demo_gb = clean_us_demo.groupby(['City', 'State Code', 'Race']).agg({'Count': 'sum'})
    us_demo_gb = us_demo_gb.unstack()
    us_demo_gb.columns = ['__'.join(col).strip() for col in us_demo_gb.columns.values]
    us_demo_gb=us_demo_gb.reset_index()
    us_demo_gb=pd.merge(us_demo_gb, clean_us_demo_merge, left_on=['City', 'State Code'], right_on=['City', 'State Code'], how='inner')
    print(f"recombined rows: {us_demo_gb.shape[0]}")
    
    
    #Merge US demographics with airport codes by city and state
    us_demo_code = pd.merge(us_demo_gb, clean_airport_code, left_on=['City', 'State Code'], right_on=['City', 'State'], how='inner')
    print(f"demo with airport codes rows: {us_demo_code.shape[0]}")
    
    
    #Merge US Demo Code with us city temp by city
    us_demo_temp = pd.merge(us_demo_code, clean_us_temp, left_on='City', right_on='City', how='inner')
    print(f"demo, airport codes, and temperature rows: {us_demo_temp.shape[0]}")
    
    #Create a 
    #Create i94 city fact table by merge us demo temp with i94
    i94_city_fact = pd.merge(us_demo_temp, clean_i94_merge, left_on='Ident', right_on='i94port', how='inner')
    print(f"demo, airport codes, temperature, and i94 rows: {i94_city_fact.shape[0]}")
    
    
    #drop unused columns listed in FACT_DROP_COLUMN
    i94_city_fact.drop(TableSchema.FACT_DROP_COLUMN, axis=1, inplace = True)
    
    
    #Print first 5 rows of all tables  
    print(i94_city_fact.head(5).transpose())
    print(location_dim.head(5))
    print(person_dim.head(5))
    print(travel_dim.head(5))
    
    
    
    
    
    
if __name__ == "__main__":
    main()
