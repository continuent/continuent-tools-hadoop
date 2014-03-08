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
# Script to generate and run map/reduce to generate materialized views 
# from staging tables.  
###########################################################################

require 'rubygems'
require 'optparse'
require 'json'
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

# Find our home. 
home = ENV["THOME"]

# Set option defaults. 
options = {}
options[:table] = ".*"
options[:schema] = ".*"
options[:metadata] = "meta.json"
options[:staging_prefix] = "stage_xxx_"
options[:log] = "materialize.out"

# Process options. 
parser = OptionParser.new { |opts|
  opts.banner = "Usage: materialize.rb [options]"
  # Most common options have short and long forms. 
  opts.on('-m', '--metadata String', 'Table metadata JSON file (meta.json)') {
    |v| options[:metadata] = v}
  opts.on('-r', '--refresh String', 'Refresh Hive tables (staging|base|all)') {
    |v| options[:refresh] = v}
  opts.on('-s', '--schema String', 'Schema name') { |v| options[:schema] = v}
  opts.on('-t', '--table String', 'Table name') { |v| options[:table] = v}
  opts.on('-v', '--verbose', 'Print verbose output') { 
    options[:verbose] = true}
  opts.on('-l', '--log String', 'Log file for detailed output') { |v|
    options[:log] = v}
  # Less common options just have long form. 
  opts.on('--staging-prefix String', 'Staging table prefix') { 
    |v| options[:staging_prefix] = v}
  # Print help. 
  opts.on('-h', '--help', 'Displays help') {
    puts opts
    exit 0
  }
}
parser.parse!

# Check arguments. 
metadata_file = options[:metadata]
if ! File.exists?(metadata_file)
  puts "Metadata file does not exist: " + metadata_file
  exit 1
elseif ! File.readable?(metadata_file)
  puts "Metadata file not readable: " + metadata_file
  exit 1
end 

verbose = options[:verbose] 

# Load table metadata from file. 
tables = Table.array_from_metadata_file(metadata_file)

# Generate and run map/reduce query for each tables. 
tables.each { |tab|
  # Print header and generate query. 
  puts "# Executing map/reduce: #{tab.to_s}"
  sql = <<EOT
ADD FILE #{home}/bin/tungsten-reduce;
FROM (  
  SELECT sbx.*
    FROM #{tab.fqn("", "stage_xxx_")} sbx
      DISTRIBUTE BY #{tab.keys} 
      SORT BY #{tab.keys},tungsten_seqno,tungsten_row_id
) map1
INSERT OVERWRITE TABLE #{tab.fqn}
  SELECT TRANSFORM(
      tungsten_opcode,tungsten_seqno,tungsten_row_id,tungsten_commit_timestamp,#{tab.columns()})
    USING 'perl tungsten-reduce -k #{tab.keys} -c tungsten_opcode,tungsten_seqno,tungsten_row_id,tungsten_commit_timestamp,#{tab.columns}'
    AS #{tab.columns_with_types};
EOT
  # Print if we are verbose. 
  if verbose
    print_start_header
    puts "### Map/Reduce Query:"
    puts sql
    print_end_header
  end

  # Echo the query to the log. 
  File.open(options[:log], "a") {|f| f.write(sql) }

  # Write query to file. 
  File.open("/tmp/hive.sql", "w") {|f| f.write(sql) }
  success = system("hive -f /tmp/hive.sql >> #{options[:log]} 2>&1")
  if success
    puts "OK"
  else
    puts "FAILED"
  end
}
puts "###"
