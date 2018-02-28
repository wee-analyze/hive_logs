--load data into a HDFS folder called "lab/data".

CREATE EXTERNAL TABLE log_raw
(
log_date DATE, 
log_time CHAR(8),
category STRING, 
method STRING, 
website STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY " "   --must use the datafiles delimiter. do NOT make up your own.
STORED AS TEXTFILE LOCATION "/lab";  
-- get table info such as it's path with DESCRIBE FORMATTED log_raw;
;


--load data into log_raw
--loading data into the table; data will disappear and no longer be in original location.
LOAD DATA INPATH "/lab/data" INTO TABLE log_raw;

-- It is also possible to make the external table connect directly to the data instead so the data is never moved by using instead
-- STORED AS TEXTFILE LOCATION "/lab/data"
;

--create internal table. NOTE: ALL INTERNAL TABLES ARE LOCATED IN HDFS "/HIVE/WAREHOUSE". ALL INTERNAL TABLES AND ASSOCIATED METADATA 
--DISAPPEARS WHEN THE CLUSTER IS SHUT DOWN.
CREATE TABLE log_clean
(
log_date DATE, 
log_time CHAR(8), 
category STRING,
method STRING, 
website STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","    --outputs data with delimiter of your choice
STORED AS TEXTFILE;

--clean data by getting rid of headers and moving from raw data into the new table
INSERT INTO log_clean SELECT log_date, log_time, category, method, website 
FROM log_raw
WHERE log_date IS NOT null AND method == "GET"; 

--write SELECT query per day which retrieves 
--date
--number visitors
--time of first visitor
--time of last visitor
SELECT log_date, COUNT(*) AS total_visitors, MIN(log_time) AS first_visit, MAX(log_time) AS last_visit
FROM log_clean
GROUP BY log_date
ORDER BY total_visitors DESC;



################################


--create new internal table that will hold popular dates.  NOTE: ALL INTERNAL TABLES ARE LOCATED IN HDFS "/HIVE/WAREHOUSE". 
--ALL INTERNAL TABLES AND ASSOCIATED METADATA DISAPPEARS WHEN THE CLUSTER IS SHUT DOWN.
CREATE TABLE popular_dates
(
log_date DATE, 
total_visitors INT, 
first_visitor STRING,
last_visitor STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE;


--fill new popular dates table with the same SELECT query as above and retrieve only url's that contain "linqtosql" and only days
--that have more than 80 days.
INSERT INTO popular_dates SELECT log_date, COUNT(*) AS total_visitors, MIN(log_time) AS first_visitor, MAX(log_time) as last_visitor
FROM log_clean
WHERE website like "%linqtosql%"  --Hive is SQL-like and uses wildcards
GROUP BY log_date HAVING total_visitors > 80
ORDER BY total_visitors DESC;
