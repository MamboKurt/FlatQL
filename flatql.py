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
from sqlite_console import SQLiteConsole


STANDARD_SUFFIX = 'csv'
STANDARD_DELIMITER = ','
STANDARD_QUOTING = csv.QUOTE_NONE
STANDARD_QUOTE_CHAR = '"'
STANDARD_DOUBLEQUOTE = False
STANDARD_ESCAPECHAR = '\\'


class FlatQL:
  def __init__(self, database_path, **fmtparams):
    self._database_path = database_path
    self._sqlite_db = sqlite3.connect(':memory:')
    self._changed = False
    self._fmtparams = fmtparams

    FlatQL.load_database(self._sqlite_db, self._database_path, **self._fmtparams)

  def start_console(self):
    zero_changes = self._sqlite_db.total_changes

    sqlite_console = SQLiteConsole(self._sqlite_db)
    sqlite_console.cmdloop("SQL Interactive Console")

    self._changed = zero_changes != self._sqlite_db.total_changes

  def query(self, sql_query, output=sys.stdout):
    zero_changes = self._sqlite_db.total_changes

    with contextlib.closing(self._sqlite_db.cursor()) as c:
      c.execute(sql_query)

      changed = zero_changes != self._sqlite_db.total_changes
      self._changed = changed or self._changed

      if c.description is not None:
        headers = [col[0] for col in c.description]
        writer = csv.writer(output)
        writer.writerow(headers)

        results = c.fetchall()
        writer.writerows(results)

  def __del__(self):
    if self._changed:
      FlatQL.save_database(self._sqlite_db, self._database_path, **self._fmtparams)
    self._sqlite_db.close()


  @staticmethod
  def load_database(database, database_path, **fmtparams):
    for table_path in glob.glob(os.path.join(database_path, '*.'+STANDARD_SUFFIX)):
      FlatQL.load_table(database, table_path, **fmtparams)

  @staticmethod
  def load_table(database, table_path, **fmtparams):
    table_name = os.path.splitext(os.path.basename(table_path))[0]

    with contextlib.closing(database.cursor()) as c, open(table_path, 'rU') as file_handle:
      csv_reader = csv.reader(file_handle, **fmtparams)
      columns = [column.strip() for column in csv_reader.next()]
      #~ Somethings fishy here. "'s get added in suspicious ways
      escaped_columns = [re.sub(r'^([^" ]+)(.*)', r'"\1"\2', column) for column in columns]

      c.execute((
        u'CREATE TABLE IF NOT EXISTS "{table_name}"(' + u', '.join([u'{}'] * len(escaped_columns)) + u');'
        ).format(table_name=table_name, *escaped_columns))

      query = u'INSERT INTO "{table_name}" VALUES ({params})'.format(
                table_name=table_name,
                params=u', '.join(u'?' * len(escaped_columns)) )

      #~ !!! ENCODING -> UNICODE
      c.executemany(query, csv_reader)

    database.commit()

  @staticmethod
  def save_database(database_connection, database_path, **fmtparams):
    for table_path in glob.glob(os.path.join(database_path, '*.'+STANDARD_SUFFIX)):
      os.unlink(table_path)

    with contextlib.closing(database_connection.cursor()) as c:
      c.execute(u'SELECT name FROM sqlite_master WHERE type = \'table\' AND name NOT LIKE \'sqlite_%\';')
      write_tables = [row[0] for row in c.fetchall()]

    for write_table in write_tables:
      FlatQL.save_table(database_connection, database_path, write_table, **fmtparams)

  @staticmethod
  def save_table(database_connection, directory_path, table_name, **fmtparams):
    csv_file_path = os.path.join(directory_path, table_name+'.csv')
    with contextlib.closing(database_connection.cursor()) as c, open(csv_file_path, 'w') as file_handle:
      c.execute(u'SELECT sql FROM sqlite_master WHERE name = "{table_name}";'.format(table_name = table_name))
      sql = c.fetchone()[0]
      columns_string = sql[sql.find("(")+1:sql.find(")")]
      writer = csv.writer(file_handle, **fmtparams)
      columns = [column.strip() for column in columns_string.split(',')]
      writer.writerow(columns)

      c.execute(
        u'SELECT * FROM {table_name}'.format(table_name=table_name) )
      results = c.fetchall()
      #~ !!! UNICODE -> ENCODING
      writer.writerows(results)


def main():
  argument_parser = argparse.ArgumentParser(
    description="Execute SQL-Queries on a Folder containing CSV Files",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  argument_parser.add_argument('-p', '--path',
    default='./',
    action=existing_path,
    help="Database Directory Path")
  argument_parser.add_argument('-q', '--query',
    help="Semicolon separated SQLite Queries. None gives you an SQLite Terminal")
  argument_parser.add_argument('--delimiter',
    default=STANDARD_DELIMITER,
    help="Delimiter in CSV Table File")
  argument_parser.add_argument('--switch_quoting',
    action='store_const',
    dest='quoting',
    default=STANDARD_QUOTING,
    const=not STANDARD_QUOTING,
    help="Quoting in CSV Table File")
  argument_parser.add_argument('--quotechar',
    default=STANDARD_QUOTE_CHAR,
    help="Quotingchar in CSV Table File")
  argument_parser.add_argument('--switch_doublequote',
    action='store_const',
    dest='doublequote',
    default=STANDARD_DOUBLEQUOTE,
    const=not STANDARD_DOUBLEQUOTE,
    help="Doublequote in CSV Table File")
  argument_parser.add_argument('--escapechar',
    default=STANDARD_ESCAPECHAR,
    help="Escapechar in CSV Table File")

  parsed_arguments = argument_parser.parse_args()
  database_path = parsed_arguments.path
  sql_query = parsed_arguments.query

  fmtparams = {key: value for key, value in vars(parsed_arguments).iteritems() if key in dir(csv.Dialect)}

  try:
    fql = FlatQL(database_path, **fmtparams)
    if sql_query is not None:
      queries = [query.strip() for query in sql_query.split(';')]
      for query in queries:
        fql.query(query)
    else:
      fql.start_console()
  except sqlite3.Error as e:
    print "SQLite Error: {0}".format(e.args[0])


if __name__ == '__main__':
  sys.exit(main())