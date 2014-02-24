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

# Define handy routine to run commands. 
def run(command, abort_on_failure=false, verbose=false)
  if verbose
    puts "Executing command: #{command}"
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
options[:map_reduce] = true
options[:compare] = true
options[:log] = "load.out"
options[:ext_libs] = "/usr/lib/hive/lib"

# Process options. 
parser = OptionParser.new { |opts|
  opts.banner = "Usage: load-reduce-check[.rb] [options]"
  # Most common options have short and long forms. 
  opts.on('-U', '--url String', 'MySQL DBMS JDBC url') { |v| options[:url] = v}
  opts.on('-u', '--user String', 'MySQL user') { |v| options[:user] = v}
  opts.on('-p', '--password String', 'MySQL password') { 
    |v| options[:password] = v}
  opts.on('-s', '--schema String', 'Schema name') { |v| options[:schema] = v}
  opts.on('-r', '--replicator String', 'Replicator home (/opt/continuent') { 
    |v| options[:replicator] = v}
  opts.on('-m', '--metadata String', 'Table metadata JSON file (/tmp/meta.json)') {
    |v| options[:metadata] = v}
  opts.on('-v', '--verbose', 'Print verbose output') { 
    options[:verbose] = true}
  opts.on('-l', '--log String', 'Log file for detailed output') { |v|
    options[:log] = v}
  # Less common options just have long form. 
  opts.on('--hive-ext-libs', 'Location of Hive JDBC jar files') {
    |v| options[:ext_libs] = v}
  opts.on('--[no-]staging-ddl', 'Load staging table ddl') {
    |v| options[:staging_ddl] = v}
  opts.on('--[no-]base-ddl', 'Load base table ddl') {
    |v| options[:base_ddl] = v}
  opts.on('--[no-]map-reduce', 'Load base table ddl') {
    |v| options[:map_reduce] = v}
  opts.on('--[no-]compare', 'Compare to source data') {
    |v| options[:compare] = v}
  # Print help.
  opts.on('-h', '--help', 'Displays help') {
    puts opts
    exit 0
  }
}
parser.parse!

# Check arguments. 
schema = options[:schema]
if ! defined? schema
  puts "Schema not defined"
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

# Load staging table definitions. 
if options[:staging_ddl]
  puts "### Generating staging table definitions"
  run("#{replicator_bin}/ddlscan -template ddl-mysql-hive-0.10-staging.vm \
      -user #{user} -pass #{password} -url #{url} -db #{schema} \
      > /tmp/staging.sql", 
    true);

  puts "### Loading staging table DDL"
  run("hive -f /tmp/staging.sql", true)
else
  puts "### Staging DDL Skipped"
end

# Load base table definitions. 
if options[:base_ddl]
  puts "### Generating and loading base table definitions"
  run("#{replicator_bin}/ddlscan -template ddl-mysql-hive-0.10.vm \
      -user #{user} -pass #{password} -url #{url} -db #{schema} \
      > /tmp/base.sql", 
    true);

  run("hive -f /tmp/base.sql", true)
else
  puts "### Base DDL Skipped"
end

# Generate metadata and run map/reduce. 
if options[:map_reduce]
  puts "### Generating table metadata"
  run("#{replicator_bin}/ddlscan -template ddl-mysql-hive-metadata.vm \
      -user #{user} -pass #{password} -url #{url} -db #{schema} \
      > #{options[:metadata]}", 
    true);

  puts "### Starting map/reduce"
  run("#{home}/bin/materialize -m #{options[:metadata]}", true)
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
       -driver org.apache.hive.jdbc.HiveDriver >> #{options[:log]} 2>&1", false)

    # Dump output if there is a failure. 
    if ok
      puts "Compare succeeded"
    else
      puts "Compare failed"
    end
  }
else
  puts "### Compare skipped"
end

# All done!
puts "Done!!!"
