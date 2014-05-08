continuent-tools-hadoop
=======================

Supplemental utilities to help generate materialized views of data 
replicated into Hadoop. This project works in conjunction
with Tungsten Replicator 3.0, which supports replication to HDFS
(https://code.google.com/p/tungsten-replicator).

# Prerequisites

You will need to install ruby, rubygems, and the Json ruby library. 

	sudo apt-get install ruby
	sudo apt-get install rubygems
	sudo gem install json

In addition you must create any Hive databases that will receive table data
from Tungsten Replicator. Here's a typical create database command: 

	hive> create databases foo;

Finally, you must set up Hadoop data loading using Tungsten Replicator.  
Replicator setup is described in the [product documentation][https://docs.continuent.com/tungsten-replicator-3.0/deployment-hadoop-operation.html] 
on the Continuent web site.

# A Quick Tutorial 

## Replicator Setup

First off you need to set up replication from MySQL into Hadoop.  This is 
documented in the product docs referenced earlier.  Confirm that replication 
is working and that you can see table data loading into a staging area.  Table
data appear as CSV files.  The following example shows files from replicator
service `mysql1` that correspond to table `db01.sbtest`.

    $ hadoop fs -ls /user/tungsten/staging/mysql1/db01/sbtest|head
    Found 804 items
    -rw-r--r--   3 tungsten supergroup     564368 2014-03-19 21:58 /user/tungsten/staging/db01/sbtest/sbtest-100016.csv
    -rw-r--r--   3 tungsten supergroup     547519 2014-03-19 21:57 /user/tungsten/staging/db01/sbtest/sbtest-10016.csv
    -rw-r--r--   3 tungsten supergroup     608107 2014-03-19 21:58 /user/tungsten/staging/db01/sbtest/sbtest-101016.csv
    -rw-r--r--   3 tungsten supergroup     519369 2014-03-19 21:57 /user/tungsten/staging/db01/sbtest/sbtest-1016.csv
    -rw-r--r--   3 tungsten supergroup     576048 2014-03-19 21:58 /user/tungsten/staging/db01/sbtest/sbtest-102016.csv
    -rw-r--r--   3 tungsten supergroup     427387 2014-03-19 21:58 /user/tungsten/staging/db01/sbtest/sbtest-103016.csv
    -rw-r--r--   3 tungsten supergroup     565180 2014-03-19 21:58 /user/tungsten/staging/db01/sbtest/sbtest-104016.csv
    -rw-r--r--   3 tungsten supergroup     565730 2014-03-19 21:58 /user/tungsten/staging/db01/sbtest/sbtest-105016.csv
    -rw-r--r--   3 tungsten supergroup     563025 2014-03-19 21:58 /user/tungsten/staging/db01/sbtest/sbtest-106016.csv
     ...

## Generating a Materialized View 

The following example shows how to create a materialized view that will 
contain all data from table sbtest as of the current position of the 
replicator.

First, you must create a schema to receive your data in Hive.  By default the
schema name has the replication service as a prefix, so you should create the
following: 

    hive> create schema mysql1_db01; 
    OK
    Time taken: 0.055 seconds

Now you can run the load-reduce-check program to populate data into your view. 

    cd git/continuent-tools-hadoop
    bin/load-reduce-check -U jdbc:mysql:thin://logos1:3306/db01 -s db01 -U tungsten -p secret --service=mysql1

If you want to see exactly what is happening, add the --verbose option. 

This command will crunch away for a while. It does the following: 

1. Create a Hive staging table definition.  This allows you to run SELECT on the replicator log records loaded to HDFS. 
2. Create a Hive table definition for the base table.  This is your materialized view that allows you to run SELECT on the table as it existed in the original DBMS server. 
3. Generate a sqoop command to provision from the original DBMS table. (It does not run the command yet.)
4. Run MapReduce to merge the staging data with the current base table
 and create a new materialized view.  
5. Run the 'dc' command to compare the DBMS table with the new table. 

If everything works properly you can look at your staging data as well as your base data, which should match the original DBMS table.  Here is an example of the type of queries you can do. 

    hive> select * from stage_xxx_sbtest limit 3;
    OK
    D	100017	1	2014-03-19 17:24:45	5030	NULL	NULL	NULL
    I	100017	2	2014-03-19 17:24:45	5030	2		aaaaaaaaaaffffffffffrrrrrrrrrreeeeeeeeeeyyyyyyyyyy
    D	100017	3	2014-03-19 17:24:45	4989	NULL	NULL	NULL
    Time taken: 1.54 seconds
    hive> select * from sbtest limit 3;
    OK
    1	0		qqqqqqqqqqwwwwwwwwwweeeeeeeeeerrrrrrrrrrtttttttttt
    2	0		qqqqqqqqqqwwwwwwwwwweeeeeeeeeerrrrrrrrrrtttttttttt
    3	0		qqqqqqqqqqwwwwwwwwwweeeeeeeeeerrrrrrrrrrtttttttttt
    Time taken: 0.117 seconds

If things do not work properly you will see on or more error messages.  Look in the load.out and materialize.out files for more information. 

# Utility Reference
## load-reduce-check

Implements the process to convert staging data to a materialized view, 
then compare the results.  The steps are: 

1. Generate and load schema for staging tables, which holds newly arrived transactions. 
2. Generate and load schema for base tables. 
3. Generate sqoop command suitable for provisioning.
4. Run map/reduce operation to generate a materialized view.  This invokes the materialize utility. 
5. Compare data using Bristlecone dc command. 

The full command interface is shown below: 

	$ bin/load-reduce-check -h
    Usage: load-reduce-check[.rb] [options]
        -D, --staging-dir String         Directory within Hadoop for staging data (default=/user/tungsten/staging)
        -l, --log String                 Log file for detailed output
        -m, --metadata String            Table metadata JSON file (/tmp/meta.json)
        -P, --schema-prefix String       Prefix for schema names (defaults to replication service
        -p, --password String            MySQL password
        -q, --sqoop-dir String           Directory within Hadoop for Sqooped table data (default=/user/tungsten/sqoop)
        -r, --replicator String          Replicator home (/opt/continuent)
        -S, --service String             Replicator service that generated data
        -s, --schema String              DBMS schema
        -t, --table String               Table within schema (default=all)
        -U, --url String                 MySQL DBMS JDBC url
        -u, --user String                MySQL user
        -v, --verbose                    Print verbose output
            --hive-ext-libs String       Location of Hive JDBC jar files
            --[no-]base-ddl              Load base table ddl
            --[no-]compare               Compare to source data
            --[no-]map-reduce            Materialize view for tables (deprecated)
            --[no-]materialize           Materialize view for tables
            --[no-]meta                  Generate metadata for tables
            --[no-]sqoop                 Generate Sqoop commands to provision data
            --[no-]staging-ddl           Load staging table ddl
        -h, --help                       Displays help

Here is an example of running the full load-reduce-check process on all tables
in a single schema db01 loaded by replication service mysql. 

	load-reduce-check -U jdbc:mysql:thin://logos1:3306/db01 -s db01 -S mysql1

The following example shows reduce with comparison only on a single table in the same schema. 
	
	load-reduce-check -U jdbc:mysql:thin://logos1:3306/db01 -s db01 -t sbtest -S mysql1 --no-staging-ddl --no-base-ddl --no-sqoop

To see full output from the process, use the --verbose option.  Otherwise most output goes to log files load.out and materialize.out.  

_NOTE:_ As of Tungsten Replicator 3.0.0 Build 59 you must always include a service name.  Failure to do so will likely lead to errors. 

## materialize

Creates HiveQL to generate materialized views and executes same using
Hive.  This utility depends on a JSON metadata file that must be provided
as an argument.  It also used the tungsten-reduce script to reduce rows.

The interface is shown below: 

	$ bin/materialize -h
	Usage: materialize.rb [options]
	    -m, --metadata String            Table metadata JSON file (meta.json)
	    -r, --refresh String             Refresh Hive tables (staging|base|all)
	    -s, --schema String              Schema name
	    -t, --table String               Table name
	    -v, --verbose                    Print verbose output
	    -l, --log String                 Log file for detailed output
	        --staging-prefix String      Staging table prefix
	    -h, --help                       Displays help

# tungsten-reduce

Implements reduce operation on updates to a particular key to create
a materialized view.  The updates are assumed to be distributed by the
row key and sorted into commit order.  The tungsten-reduce script emits
the last row for each key if it is an insert.  Deletes are dropped.
