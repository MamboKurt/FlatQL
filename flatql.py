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

import unicode_csv

from tools import existing_directory, existing_file
from sqlite_console import SQLiteConsole


DEFAULT_SUFFIX = 'csv'
DEFAULT_FILE_ENCODING = 'utf-8'
DEFAULT_DELIMITER = ','
DEFAULT_QUOTING = csv.QUOTE_NONE
DEFAULT_QUOTE_CHAR = '"'
DEFAULT_DOUBLEQUOTE = False
DEFAULT_ESCAPECHAR = '\\'

QUOTING_DICT = {'none': csv.QUOTE_NONE,
                'minimal': csv.QUOTE_MINIMAL,
                'nonnumeric': csv.QUOTE_NONNUMERIC,
                'all': csv.QUOTE_ALL}


class FlatQL:
  def __init__(self, database_path, **fmtparams):
    self._database_path = database_path
    self._sqlite_db = sqlite3.connect(':memory:')
    self._changed = False
    self._fmtparams = fmtparams

    FlatQL.load_database(self._sqlite_db, self._database_path, **self._fmtparams)

  def start_console(self):
    sqlite_console = SQLiteConsole(self._sqlite_db)
    sqlite_console.cmdloop("SQL Interactive Console")

    self._changed = sqlite_console.changed

  def query(self, sql_query, output=sys.stdout):
    with contextlib.closing(self._sqlite_db.cursor()) as c:
      c.execute(sql_query)

      if c.description is not None:
        headers = [col[0] for col in c.description]
        writer = unicode_csv.Writer(output)
        writer.writerow(headers)

        results = c.fetchall()
        writer.writerows(results)
      else:
        self._changed = True

  def __del__(self):
    if self._changed:
      FlatQL.save_database(self._sqlite_db, self._database_path, **self._fmtparams)
    self._sqlite_db.close()


  @staticmethod
  def load_database(database, database_path, **fmtparams):
    for table_path in glob.glob(os.path.join(database_path, '*.'+DEFAULT_SUFFIX)):
      FlatQL.load_table(database, table_path, **fmtparams)

  @staticmethod
  def load_table(database, table_path, **fmtparams):
    table_name = os.path.splitext(os.path.basename(table_path))[0]

    with contextlib.closing(database.cursor()) as c, open(table_path, 'rU') as file_handle:
      csv_reader = unicode_csv.Reader(file_handle, **fmtparams)
      columns = [column.strip() for column in csv_reader.next()]

      c.execute((
        u'CREATE TABLE IF NOT EXISTS "{table_name}"(' + u', '.join([u'{}'] * len(columns)) + u');'
        ).format(table_name=table_name, *columns))

      query = u'INSERT INTO "{table_name}" VALUES ({params})'.format(
                table_name=table_name,
                params=u', '.join(u'?' * len(columns)) )

      c.executemany(query, csv_reader)

    database.commit()

  @staticmethod
  def save_database(database_connection, database_path, **fmtparams):
    for table_path in glob.glob(os.path.join(database_path, '*.'+DEFAULT_SUFFIX)):
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
      writer = unicode_csv.Writer(file_handle, **fmtparams)
      columns = [column.strip() for column in columns_string.split(',')]
      writer.writerow(columns)

      c.execute(
        u'SELECT * FROM {table_name}'.format(table_name=table_name) )
      results = c.fetchall()
      writer.writerows(results)


def main():
  argument_parser = argparse.ArgumentParser(
    description="Execute SQL-Queries on a Folder containing CSV Files",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  argument_parser.add_argument('-p', '--path',
    default='./',
    action=existing_directory,
    help="Database Directory Path")
  query_group = argument_parser.add_mutually_exclusive_group()
  query_group.add_argument('-q', '--query',
    help="Semicolon separated SQLite Queries.")
  query_group.add_argument('-s', '--sqlscript',
    action=existing_file,
    help="Semicolon separated SQLite Queries in a File.")
  argument_parser.add_argument('--delimiter',
    default=DEFAULT_DELIMITER,
    help="Delimiter in CSV Table File")
  argument_parser.add_argument('--quoting',
    choices=QUOTING_DICT.keys(),
    default='minimal',
    dest='quoting_string',
    help="Quoting in CSV Table File")
  argument_parser.add_argument('--quotechar',
    default=DEFAULT_QUOTE_CHAR,
    help="Quotingchar in CSV Table File")
  argument_parser.add_argument('--'+('no' if DEFAULT_DOUBLEQUOTE else '')+'doublequote',
    action='store_const',
    dest='doublequote',
    default=DEFAULT_DOUBLEQUOTE,
    const=not DEFAULT_DOUBLEQUOTE,
    help=('Disable ' if DEFAULT_DOUBLEQUOTE else '')+"Doublequote in CSV Table File")
  argument_parser.add_argument('--escapechar',
    default=DEFAULT_ESCAPECHAR,
    help="Escapechar in CSV Table File")

  parsed_arguments = argument_parser.parse_args()
  database_path = parsed_arguments.path

  fmtparams = {key: value for key, value in vars(parsed_arguments).iteritems() if key in dir(csv.Dialect)}
  fmtparams['quoting'] = QUOTING_DICT[parsed_arguments.quoting_string]

  if parsed_arguments.query is not None:
    query_string = parsed_arguments.query
  elif parsed_arguments.sqlscript is not None:
    with open(parsed_arguments.sqlscript, "r") as file_handle:
      query_string = file_handle.read()

  try:
    fql = FlatQL(database_path, **fmtparams)
    if query_string is not None:
      queries = filter(bool, [query.strip() for query in query_string.split(';')])
      for query in queries:
        fql.query(query)
    else:
      fql.start_console()
  except sqlite3.Error as e:
    print "SQLite Error: {0}".format(e.args[0])


if __name__ == '__main__':
  sys.exit(main())