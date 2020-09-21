# UDACITY-Data-Engineering-Capstone-Project

#### My project code is all located in a file labeled etl.py

The goal of this project is to take Immigration data pair it with average temperature and population counts grouped by city and state (where feasible). 
Here is the main query I will run.
Select average temperature, total population, Median age from fact table where city = ‘Salt Lake City’
Here is an example of a simple query
SELECT AverageTemperature, Total Population, Median Age
 FROM i94_city_fact
WHERE City = ‘Salt Lake City’

I will use Apache Spark and pandas to create dataframes to import, clean and create new fact and dimension tables in a star schema.  This optimizes queries on city immigration, demographics, and average temperature analysis.

I chose Apache Spark for this project because I wanted to utilize the fast and easy pandas dataframe. More specifically I wanted to use the groupby, aggregate, unstack, and reindex functions to help group by city and state and convert the race type rows of the us demographic table to separate columns to wrangle the data into an easier queryable format. 

The first step is to load all the data into separate dataframe tables. The number of rows is printed for each data source Then it gets cleaned from null values and remove duplicate rows. The number of rows and number of dropped rows is printed. This helps with data quality checks. If a lot of rows are removed something is wrong and steps will need to be taken to evaluate where the problem is.  Some data like the us demographics and us temperature needs to be wrangled into a new form. Then the data is merged into location, person, and travel dimension tables and one fact table. 

The world temperatures data is based on monthly data. The data should be updated monthly because that is how often the world temperatures data is updated.

If the data increased by 100x I would partition the data by date. Then load each partition separately.  Parrallel processing if feasible. Distribute across multiple clusters

If the pipelines were run on a daily basis by 7am. Apache airflow can schedule and run the ETL Program and set run time quality checks including alerts when task takes longer than expected.

If the database needed to be accessed by 100+ people. I would use a cloud data storage service that caches images at different data centers across the world.  Look at ways to improve reads from the database by splitting tables.

 






