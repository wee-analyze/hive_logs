# hive_logs
A simple example of using Hive in Hadoop to fill and query semi-structured log data into tables

Programming language: HiveQL

Hive is another SQL-like interface used in Hadoop for Big Data processing using MapReduce. Tables are created that describes
the data and is imposed on metadata that is stored HDFS (schema-on-read); opposite of sql which is schema-on-write. Then, 
SQL commands can be used to make queries using MapReduce.
The Hive tables that are created are stored in the Hive metastore.
There are 2 types of tables that can be created: internal and external tables.
Internal tables are stored in the HDFS directory /hive/warehouse and disappear when the Hadoop cluster is shut down.
External tables are stored in created directories and remain in HDFS even after a Hadoop cluster is shut down.

The data used for this example is a small log sample and is semi-structured. Even if the sample was very large, terabytes, 
Hadoop would be able to handle the processing with MapReduce. This script will perform the following:

-filter out all url's that contain "linqtosql"

-retrieve only days with more than 80 visits

-sort the number of visits and then by date if visits are equal

To run this script, make a directory in HDFS called "/data" and load log data sample 2018_jan.txt.
This script script created an external table and 
