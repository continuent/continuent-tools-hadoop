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

require 'rubygems'
require 'json'
require 'fileutils'

# Define class to hold metadata and provides convenience methods for printing
# table information. 
class Table
  # Define accessors for selected instance values. 
  attr_reader :schema, :name

  def initialize(defining_hash)
    @table_meta = defining_hash
    @schema = @table_meta["schema"]
    @name = @table_meta["name"]
    @keys = @table_meta["keys"]
    @columns = @table_meta["columns"]
  end

  # Load table metadata and parse JSON input to return a list of one or more
  # tables. 
  def Table.array_from_metadata_file(metadata_file)
    # Load from file and parse to hash structure. 
    json_file = open(metadata_file)
    json = json_file.read
    metadata = JSON.parse(json)

    # Load tables to an array. 
    tables = Array.new
    metadata["tables"].each { |meta|
      tab = Table.new(meta)
      tables << tab
    }
    tables
  end

  # Return fully-qualified table name. 
  def fqn(schema_prefix="", name_prefix="")
    "#{schema_prefix}#{@schema}.#{name_prefix}#{@name}"
  end

  def keys
    @keys.join(",")
  end

  def columns
    cols = Array.new
    @columns.each {|col|
      cols << col["name"]
    }
    cols.join(",")
  end

  def columns_with_types
    cols = Array.new
    @columns.each {|col|
      cols << col["name"] + " " + col["type"]
    }
    cols.join(",")
  end

  def to_s
    "Table: schema=#{@schema} name=#{@name} keys=#{keys}"
  end
end
