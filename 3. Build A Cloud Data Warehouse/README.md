# Data Warehouse

## Project Overview
Sparkify is a startup that develops a new music streaming app. It wants to analyze the data they've been collecting on their app. 

There are two data sets (songs and log information) residing on Amazon S3 Buckets.


## Project outcome 
The purpose of this project is to support the analytics team with their analystical goals. 

It will extract data sets from multiple S3 buckets and load it into staging tables in a database hosted on Redshift. 

The data in staging tables will be re-extracted and loaded into the various tables for analysis. 

## Project instructions
<ol> 
    <li> Create a redshift cluster on AWS </li> 
    <li> Create a IAM user, grant AdministratorAccess, attach policies, IAM role that has read access to S3</li>
    <li> Fill up the connection details (endpoint as host, database credentials, ARN) in dwh.cfg </li>
    <li> Execute create_tables.py to create and drop existing tables in the database </li> 
    <li> Execute etl.py to process the ETL pipeline to insert data into the database
        
</ol>

## Databse Schema
The following tables will be created upon executing create_tables.py

| Table | Purpose of table |   
|-------|------|
| staging_events| staging table for event data ( resides in S3 ) |  
| staging_songs | staging table for song data ( resides in S3 )|   
| songplays | information on what song ( title, artist ) was played by which user and in which session|   
| users | user related information ( name, gender, level) | 
| songs | song related information ( title, artist, year, duration ) |
| artists | artist related information ( name, location ) |
| time | time related information | 

### Staging tables
<ul> 
    <li>staging_events
        <ul> <li> event_id, artist_name,
       auth, user_first_name, user_gender,
       item_in_session, user_last_name,
       song_length, level, location,
       method, page, registration,
       session_id, song_title,
       status, ts, user_agent, user_id </li></ul>  
    <li>staging_songs
        <ul> <li> song_id, num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, title, duration, year </li></ul>      
</ul>
        
### Fact table 
<ul> 
    <li> songplays
      <ul> <li> start_time, user_id, level, song_id, artist_id, session_id, location, user_agent </li></ul>
</ul>

### Dimension tables 
<ul> 
    <li> users
        <ul><li>user_id, first_name, last_name, gender, level</li></ul></li> 
     <li> songs
        <ul><li>song_id, title , artist_id, year , duration</li></ul></li> 
     <li> artists
        <ul><li>artist_id , name , location, latitude, longitude</li></ul></li> 
     <li> time
        <ul><li>start_time, hour , day , week , month , year ,weekday </li></ul></li> 
</ul>

### Example queries 
<ul> 
    <li> Total number of users
         <ul> <li>select COUNT(*) AS total FROM users </li></ul></li> 
    <li> Total number of artists
         <ul> <li>  select COUNT(*) AS total FROM artists </li></ul></li> 
</ul> 


### Delete the cluster 
Delete your redshift cluster when finished.