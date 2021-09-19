# Data Warehouse

## Project Overview
Sparkify is a startup that develops a new music streaming app. It wants to analyze the data they've been collecting on their app.  

To support business growth and scaling up, Sparkify wants to analyse their data in a more efficient manner as their existing data warehouse will not have the sufficient resources to handle the massive data.

Upon executing etl.py, S3 data sources will be loaded into Spark dataframes, data will be transformed into table scheme and writes back into S3 in the parquet format.


## Data
The input data contains two S3 buckets:
<ol> 
    <li>Song data: s3://udacity-dend/song_data </li> 
    <li>Log data: s3://udacity-dend/log_data </li> 
</ol>   


## Project instructions
<ol> 
    <li> Create cluster in EMR service, and bucket to store files</li>
    <li> Create an AWS IAM role with S3 that has read and write access </li> 
    <li> Enter the IAM's credentials in the dl.cfg configuration file.</li>
    <li> Replace the value of the variable 'input_data'  and 'output_data' in etl.py to point to your data files</li>
    <li> Execute etl.py to process the data </li> 
        
</ol>

## Generated tables
The following tables will be created and filled with table upon executing etl.py

| Table | Type | Columns | Purpose of table |  
|-------|------|---------|------------------| 
| songplays | fact table | songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, year, month |information on what song was played by which user and in which session| 
| songs | dimension table| song_id, title, artist_id, year, duration | song related information |
| artists | dimension table| artist_id, artist_name, artist_location, artist_lattitude, artist_longitude |  artist related information |
| users | dimension table | userId, firstName, lastName, gender, level | user related information |
| time |  dimension table  | start_time, hour, day, week, month, year, weekday | time related information | 



## Delete the cluster 
Delete your cluster when finished to prevent additional charges.