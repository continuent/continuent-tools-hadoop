continuent-tools-hadoop
=======================

Supplemental utilities to help generate materialized views of data 
replicated into Hadoop. This project works in conjunction
with Tungsten Replicator 3.0, which supports replication to HDFS
(https://code.google.com/p/tungsten-replicator).

# load-reduce-check

Implements the process to convert staging data to a materialized view, 
then compare the results.  The steps are: 

1. Generate and load schema for staging tables, which holds newly arrived transactions. 
1. Generate and load schema for base tables. 
1. Run map/reduce operation to generate a materialized view.  This invokes the materialize utility. 
1. Compare data using Bristlecone dc command. 

The inteface is shown below: 

	$ bin/load-reduce-check -h
	Usage: load-reduce-check[.rb] [options]
	    -U, --url String                 MySQL DBMS JDBC url
	    -u, --user String                MySQL user
	    -p, --password String            MySQL password
	    -s, --schema String              Schema name
	    -r, --replicator String          Replicator home (/opt/continuent
	    -m, --metadata String            Table metadata JSON file (/tmp/meta.json)
	    -v, --verbose                    Print verbose output
	    -l, --log String                 Log file for detailed output
	        --hive-ext-libs              Location of Hive JDBC jar files
	        --[no-]staging-ddl           Load staging table ddl
	        --[no-]base-ddl              Load base table ddl
	        --[no-]map-reduce            Load base table ddl
	        --[no-]compare               Compare to source data 
	    -h, --help                       Displays help

# materialize

Creates HiveQL to generate materialized views and executes same using Hive.  This utility depends on a JSON metadata file that must be provided as an argument.  It also used the tungsten-reduce script to reduce rows. 

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

Implements reduce operation on updates to a particular key to create a materialized view.  The updates are assumed to be distributed by the row key and sorted into commit order.  The tungsten-reduce script emits the last row for each key if it is an insert.  Deletes are dropped. 
