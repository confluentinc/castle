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

#
# castle_graph_csv.py
#
# The castle tool allows users to set up collectd on the local nodes to
# gather system statistics.  This script graphs the CSV files which are
# generated by that service.
#

class CsvFile(object):
    def __init__(self, path, column_names):
        self.path = path
        self.basename = os.path.basename(path)
        with open(path, 'r') as input_file:
            reader = csv.reader(input_file, delimiter=',')
            self.set_up_title_row(reader.__next__(), column_names)
            if len(self.title_row) < 1:
                raise RuntimeError("Expected at least one columns in the first \
line of the csv file %s" % path)
            self.first_data_row = self.filter_row(reader.__next__())
            if len(self.title_row) != len(self.first_data_row):
                raise RuntimeError("Expected the length of the title row to be \
the same as the length of the first data row for %s" % path)
            self.first_timestamp = float(self.first_data_row[0])
            self.columns = []
            for i in self.needed_rows:
                self.columns.append(CsvColumn(self, self.needed_rows[i],
                                              "%d.%s.%s" % (i, self.basename, self.title_row[i])))

    def set_up_title_row(self, row, column_names):
        """
        Filter the title row so that it only contains the column names
        we want.
        """
        if column_names is None or len(column_names) == 0: # allow all
            self.needed_rows = range(0, len(row))
            self.title_row = row
        else:
            self.title_row = [row[0]]
            self.needed_rows = [0]
            for i in range(1, len(row)):
                if row[i] in column_names:
                    self.title_row.append(row[i])
                    self.needed_rows.append(i)


    def filter_row(self, row):
        filtered_row = []
        for i in range(0, len(row)):
            if i in self.needed_rows:
                filtered_row.append(row[i])
        return filtered_row
        


class CsvColumn(object):
    def __init__(self, csv_file, column_index, name):
        self.csv_file = csv_file
        self.column_index = column_index
        self.name = name

    def values(self, shift_time_axis, derivative):
        with open(self.csv_file.path, 'r') as input_file:
            reader = csv.reader(input_file, delimiter=',')
            reader.__next__()
            if derivative:
                reader.__next__()
            prev_value = float(self.csv_file.first_data_row[self.column_index])
            prev_timestamp = self.csv_file.first_timestamp
            for row in reader:
                timestamp = float(row[0])
                value = float(row[self.column_index])

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
    '-c', '--column-name', dest='column_name', action='append',
    help="Set the column names we should graph.  May be specified more than once.")
parser.add_argument(
    '-D', '--derivative', dest='derivative', action='store_true',
    help="Take the derivative of the value.")
parser.add_argument('csv_files', nargs='*', help="Paths to the csv files to graph.")
cmd_args = vars(parser.parse_args())

for path in cmd_args["csv_files"]:
    csv_file = CsvFile(path, cmd_args["column_name"])
    for column in csv_file.columns:
        x = []
        y = []
        for (timestamp, data) in column.values(cmd_args["shift_time_axis"],
                                               cmd_args["derivative"]):
            x.append(datetime.datetime.fromtimestamp(timestamp))
            y.append(data)
        plt.plot(x, y, label=column.name)
if cmd_args["name"] is None:
    plt.title('Castle Test Graphs')
else:
    plt.title(str(cmd_args["name"]))
plt.xlabel('time')
plt.legend()
plt.show()