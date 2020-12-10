# Sparkify Database and ETL

## What is Sparkify?

Sparkify, a music streaming startup, wanted to collect logs they have on user activity and song data and centralize them in a database in order to run analytics. This Postgres database, set up with a star schema, will help them to easily access their data in an intuitive fashion and start getting rich insights into their user base.

## Why this Database and ETL design?

Our fact table "songplays" contains foreign keys to our dimension table. Our dimension tables contain the descriptive elements like times, durations and other measurements of our data.

The ETL pipeline that was implemented helps quickly move Sparkify's logs to the database by combing through all the log files, parsing out relevant information for each table and then inserting that into the database. The process is expedited even further by utilizing the psycopg2 'copy_from' function which batch copies groups of records into the table instead of adding them 1 by 1. 

## Example song analysis queries

Find area with highest amount of listening instances:  
`SELECT location, count(location) as num_of_listeners FROM songplays group by location order by num_of_listeners desc limit 1;`  
**Result: San Francisco-Oakland-Hayward, CA, 691**

Find amount of paying users:
`SELECT count(level) FROM users WHERE level = 'paid';`  
**Result: 5591**
