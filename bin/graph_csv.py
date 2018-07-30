#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import csv
import datetime
import matplotlib.pyplot as plt
import os
import sys

#
# castle_graph_csv.py
#
# The castle tool allows users to set up collectd on the local nodes to
# gather system statistics.  This script graphs the CSV files which are
# generated by that service.
#

class ColumnSelector(object):
    """
    Represents a column selector, which chooses a CSV column.
    """
    def __init__(self, name):
        self.name = name
        self.found = False


class CsvFile(object):
    def __init__(self, path):
        self.path = path


class CsvFile(object):
    def __init__(self, path):
        self.path = path
        self.basename = os.path.basename(path)
        with open(path, 'r') as input_file:
            reader = csv.reader(input_file, delimiter=',')
            quoted_title_row = reader.__next__()
            self.title_row = []
            for entry in quoted_title_row:
                self.title_row.append(entry.strip().strip('"'))
            if len(self.title_row) < 1:
                raise RuntimeError("Expected at least one column in the first \
line of the csv file %s" % path)
            self.first_data_row = reader.__next__()
            if len(self.title_row) != len(self.first_data_row):
                raise RuntimeError("Expected the first data row for %s to have %d \
elements to match the title row.", path, len(self.title_row))
            self.first_timestamp = float(self.first_data_row[0])
            self.columns = {}
            for i in range(1, len(self.title_row)):
                column_title = self.title_row[i]
                self.columns[column_title] = CsvColumn(self, i)

    def column_names(self):
        return self.title_row[1:]

    def get_column(self, name):
        if name in self.columns:
            return self.columns[name]
        else:
            return None


class CsvColumn(object):
    def __init__(self, csv_file, index):
        self.csv_file = csv_file
        self.index = index

    def pretty_name(self):
        return "%d.%s.%s" % (self.index,
                             self.csv_file.basename,
                             self.csv_file.title_row[self.index])

    def monotonic(self):
        prev_value = None
        for value in self.values(False, False):
            if prev_value is not None:
                if prev_value > value:
                    return False
            prev_value = value
        return True


    def values(self, shift_time_axis, derivative):
        with open(self.csv_file.path, 'r') as input_file:
            reader = csv.reader(input_file, delimiter=',')
            reader.__next__()
            if derivative:
                reader.__next__()
            prev_value = float(self.csv_file.first_data_row[self.index])
            prev_timestamp = self.csv_file.first_timestamp
            for row in reader:
                timestamp = float(row[0])
                value = float(row[self.index])

                if derivative:
                    effective_value = (value - prev_value) / (timestamp - prev_timestamp)
                else:
                    effective_value = value

                if shift_time_axis:
                    effective_timestamp = timestamp - self.csv_file.first_timestamp
                else:
                    effective_timestamp = timestamp

                prev_timestamp = timestamp
                prev_value = value

                yield (effective_timestamp, effective_value)


parser = argparse.ArgumentParser(
    description='Script to graph comma-separated files generated during a castle test run.')
parser.add_argument(
    '-n', dest='name', action="store",
    help="Set the name of the graph.")
parser.add_argument(
    '-s', '--shift-time-axis', dest='shift_time_axis', action='store_true',
    help="Shift the time axis so that all graphs begin at the same time.")
parser.add_argument(
    '-c', '--column-name', dest='column_names', action='append',
    help="Set the column names we should graph.  May be specified more than once.")
parser.add_argument(
    '-d', '--derivative', dest='derivative', action='store_true',
    help="Take the derivative of all columns.")
parser.add_argument(
    '-D', '--no-derivative', dest='no_derivative', action='store_true',
    help="Disable taking the derivative for all columns.")
parser.add_argument('csv_files', nargs='*', help="Paths to the csv files to graph.")
cmd_args = vars(parser.parse_args())

if len(cmd_args["csv_files"]) == 0:
    parser.print_help(sys.stdout)
    sys.exit(0)

# Load CSV files.
csv_files = []
column_names = {}
for path in cmd_args["csv_files"]:
    csv_file = CsvFile(path)
    csv_files.append(csv_file)
    for column_name in csv_file.column_names():
        column_names[column_name] = True

# Load column selectors.
column_selectors = []
def print_column_names():
    print("Found column(s):")
    for name in column_names:
        print("    " + name)
if cmd_args["column_names"] is None or len(cmd_args) == 0:
    print_column_names()
    print("Please specify the columns with -c")
    sys.exit(0)
for column_name in cmd_args["column_names"]:
    selector = ColumnSelector(column_name)
    column_selectors.append(selector)
    if selector.name not in column_names:
        print("Unable to find column " + selector.name)
        print_column_names()
        sys.exit(1)

# Populate the graph.
for csv_file in csv_files:
    for selector in column_selectors:
        column = csv_file.get_column(selector.name)
        if column is not None:
            x = []
            y = []
            if cmd_args["derivative"]:
                derivative = True
            elif cmd_args["no_derivative"]:
                derivative = False
            else:
                derivative = column.monotonic()
            for (timestamp, data) in column.values(cmd_args["shift_time_axis"],
                                                   derivative):
                x.append(datetime.datetime.fromtimestamp(timestamp))
                y.append(data)
            plt.plot(x, y, label=column.pretty_name())
if cmd_args["name"] is None:
    plt.title('Castle Test Graphs')
else:
    plt.title(str(cmd_args["name"]))
plt.xlabel('time')
plt.legend()
plt.show()
