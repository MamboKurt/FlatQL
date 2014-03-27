#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import contextlib
import glob
import argparse
import re
import csv
import sqlite3

from tools import existing_path


class FlatQL:
  def __init__(self, database_path):
    self._database_path = database_path
    self._sqlite_db = sqlite3.connect(':memory:')

    FlatQL.load_database(self._sqlite_db, self._database_path)

  @staticmethod
  def load_database(database, database_path):
    for table_path in glob.glob(os.path.join(database_path, '*.csv')):
      FlatQL.load_table(database, table_path)

  @staticmethod
  def load_table(database, table_path):
    table_name = os.path.splitext(os.path.basename(table_path))[0]
      
    with contextlib.closing(database.cursor()) as c, open(table_path, 'rU') as file_handle:
      csv_reader = csv.reader(file_handle)
      columns = [column.strip() for column in next(csv_reader)]
      escaped_columns = [re.sub(r'^([^" ]+)(.*)', r'"\1"\2', column) for column in columns]
      
      c.execute((
        u'CREATE TABLE IF NOT EXISTS "{table_name}"(' +
        u', '.join([u'{}'] * len(escaped_columns)) +
        u');'
      ).format(table_name=table_name, *escaped_columns))

      query = u'INSERT INTO "{table_name}" VALUES ({params})'.format(
                table_name=table_name,
                params=u', '.join(u'?' * len(columns)) )

      #~ !!! ENCODING -> UNICODE
      c.executemany(query, csv_reader)

    database.commit()



def main():
  argument_parser = argparse.ArgumentParser(
    description="Execute SQL-Queries on a Folder" +
      " containing CSV Files")
  argument_parser.add_argument('-p', '--path',
    default='./',
    action=existing_path,
    help="Database Directory Path")

  parsed_arguments = argument_parser.parse_args()
  database_path = parsed_arguments.path
  
  flq = FlatQL(database_path)


if __name__ == '__main__':
  sys.exit(main())