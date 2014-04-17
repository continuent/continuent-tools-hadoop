#!/usr/bin/ruby
# Copyright (C) 2014 Continuent, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.  You may obtain
# a copy of the License at
# 
#         http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# Initial developer(s): Robert Hodges
# Contributor(s): 

###########################################################################
# Script to define Hive staging and base tables, run map/reduce script
# to create materialized views, and compare data from MySQL against data
# loaded to Hadoop.  
###########################################################################

require 'rubygems'
require 'optparse'
require 'fileutils'
require 'hadoop/table'

# Print a starting header. 
def print_start_header()
  puts ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
end

# Print an ending header. 
def print_end_header()
  puts "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
end

# Define handy routine to run commands. 
def run(command, abort_on_failure=false, verbose=false)
  if verbose
    print_start_header
    puts "Executing command: #{command}"
    print_end_header
  end
  success = system(command)
  if success
    puts "OK"
    true
  else
    puts "COMMAND FAILED: #{command}"
    if abort_on_failure
      raise "Terminating after failure"
    end
    false
  end
end

# Find our home. 
home = ENV["THOME"]

# Set option defaults. 
options = {}
options[:url] = "jdbc:mysql:thin://local:3306"
options[:user] = "tungsten"
options[:password] = "secret"
options[:replicator] = "/opt/continuent"
options[:metadata] = "/tmp/meta.json"
options[:staging_ddl] = true
options[:base_ddl] = true
options[:materialize] = true
options[:genmetadata] = true
options[:compare] = true
options[:sqoop] = true
options[:log] = "load.out"
options[:ext_libs] = "/usr/lib/hive/lib"
options[:sqoop_dir] = "/user/tungsten/sqoop"

# Process options. 
parser = OptionParser.new { |opts|
  opts.banner = "Usage: load-reduce-check[.rb] [options]"
  # Most common options have short and long forms. 
  opts.on('-U', '--url String', 'MySQL DBMS JDBC url') { |v| options[:url] = v}
  opts.on('-u', '--user String', 'MySQL user') { |v| options[:user] = v}
  opts.on('-p', '--password String', 'MySQL password') { 
    |v| options[:password] = v}
  opts.on('-s', '--schema String', 'Schema name') { |v| options[:schema] = v}
  opts.on('-t', '--table String', 'Table within schema (default=all)') {
    |v| options[:table] = v}
  opts.on('-q', '--sqoop-dir String', "Directory within Hadoop for Sqooped table data (default=#{options[:sqoop_dir]})") { 
    |v| options[:sqoop_dir] = v}
  opts.on('-r', '--replicator String', "Replicator home (#{options[:replicator]})") { 
    |v| options[:replicator] = v}
  opts.on('-m', '--metadata String', "Table metadata JSON file (#{options[:metadata]})") {
    |v| options[:metadata] = v}
  opts.on('-v', '--verbose', 'Print verbose output') { 
    options[:verbose] = true}
  opts.on('-l', '--log String', 'Log file for detailed output') { |v|
    options[:log] = v}
  # Less common options just have long form. 
  opts.on('--hive-ext-libs String', 'Location of Hive JDBC jar files') {
    |v| options[:ext_libs] = v}
  opts.on('--[no-]staging-ddl', 'Load staging table ddl') {
    |v| options[:staging_ddl] = v}
  opts.on('--[no-]base-ddl', 'Load base table ddl') {
    |v| options[:base_ddl] = v}
  opts.on('--[no-]materialize', 'Materialize view for tables') {
    |v| options[:materialize] = v}
  opts.on('--[no-]map-reduce', 'Materialize view for tables (deprecated)') {
    |v| options[:materialize] = v}
  opts.on('--[no-]meta', 'Generate metadata for tables') {
    |v| options[:genmetadata] = v}
  opts.on('--[no-]compare', 'Compare to source data') {
    |v| options[:compare] = v}
  opts.on('--[no-]sqoop', 'Generate Sqoop commands to provision data') {
    |v| options[:sqoop] = v}
  # Print help.
  opts.on('-h', '--help', 'Displays help') {
    puts opts
    exit 0
  }
  if options[:materialize] or options[:sqoop] or options[:compare]
    options[:genmetadata]
  end
}
parser.parse!

# Check arguments. 
schema = options[:schema]
if ! defined? schema
  puts "A schema is required to select tables"
  exit 1
end 

replicator_bin = options[:replicator] + "/tungsten/tungsten-replicator/bin"
if ! File.directory?(replicator_bin)
  puts "Replicator bin directory does not exist: " + replicator_bin
  exit 1
end 
bristlecone_bin = options[:replicator] + "/tungsten/bristlecone/bin"
if ! File.directory?(bristlecone_bin)
  puts "Bristlecone bin directory does not exist: " + bristlecone_bin
  exit 1
end 

url = options[:url]
user = options[:user]
password = options[:password]
verbose = options[:verbose]

# Selection by table is optional, so prepare a possibly empty option for
# ddlscan. 
if options[:table]
  table_opt = "-tables #{schema}.#{options[:table]}"
else
  table_opt = ""
end

# Load staging table definitions. 
if options[:staging_ddl]
  puts "### Generating staging table definitions"
  run("#{replicator_bin}/ddlscan -template ddl-mysql-hive-0.10-staging.vm \
      -user #{user} -pass #{password} -url #{url} -db #{schema} #{table_opt} \
      > /tmp/staging.sql", 
    true, verbose);
  if verbose
    print_start_header
    puts "### Staging table SQL:"
    system("cat /tmp/staging.sql")
    print_end_header
  end

  puts "### Loading staging table DDL"
  run("hive -f /tmp/staging.sql", true, verbose)
else
  puts "### Staging DDL Skipped"
end

# Load base table definitions. 
if options[:base_ddl]
  puts "### Generating and loading base table definitions"
  run("#{replicator_bin}/ddlscan -template ddl-mysql-hive-0.10.vm \
      -user #{user} -pass #{password} -url #{url} -db #{schema} #{table_opt} \
      > /tmp/base.sql", 
    verbose);
  if verbose
    print_start_header
    puts "### Base  table SQL:"
    system("cat /tmp/base.sql")
    print_end_header
  end

  run("hive -f /tmp/base.sql", verbose)
else
  puts "### Base DDL Skipped"
end

# Generate the metadata file
if options[:genmetadata]
  puts "### Generating table metadata"

  run("#{replicator_bin}/ddlscan -template ddl-mysql-hive-metadata.vm \
      -user #{user} -pass #{password} -url #{url} -db #{schema} #{table_opt} \
      > #{options[:metadata]}", 
      true, verbose);
end

# Generate sqooop commands.
if options[:sqoop]
  puts "### Generating Sqoop Provision Commands"
  tables = Table.array_from_metadata_file(options[:metadata])

  tables.each { |tab|
    File.open('/tmp/sqoop.sh','w') { |file| file.write("sqoop import --connect #{url} --username #{user} --password #{password} --table #{tab.name} --hive-import --hive-table #{tab.schema}.#{tab.name} --target-dir #{options[:sqoop_dir]}") }
  }

  if verbose
    print_start_header
    puts "### Sqoop Provisioning Commands"
    system("cat /tmp/sqoop.sh")
    print_end_header
  end
else
   puts "### Sqoop Provision Commands Skipped"
end

# Generate metadata and run map/reduce. 
if options[:map_reduce]

  if verbose 
    verbose_option = "--verbose"
  else
    verbose_option = ""
  end

  puts "### Starting map/reduce"
  run("#{home}/bin/materialize -m #{options[:metadata]} #{verbose_option}", 
    true, verbose)
else
  puts "### Map/reduce skipped"
end

# Compare data. 
if options[:compare]
  # First we need table metadata. 
  tables = Table.array_from_metadata_file(options[:metadata])

  # Now we compare each table. 
  tables.each { |tab|
    # Print header and compare
    puts "### Comparing table: #{tab.to_s}"
    ENV["TUNGSTEN_EXT_LIBS"] = options[:ext_libs]
    ok = run("#{bristlecone_bin}/dc -url1 #{url} -user1 #{user} \
       -password1 #{password} -url2 jdbc:hive2://localhost:10000 \
       -user2 'tungsten' -password2 'secret' -schema #{tab.schema} \
       -table #{tab.name} -verbose -keys #{tab.keys} \
       -driver org.apache.hive.jdbc.HiveDriver >> #{options[:log]} 2>&1", false, verbose)

    # Dump output if there is a failure. 
    if ok
      puts "COMPARE SUCCEEDED"
    else
      puts "COMPARE FAILED"
    end
  }
else
  puts "### Compare skipped"
end

# All done!
puts "Done!!!"
