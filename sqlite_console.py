# -*- coding: utf-8 -*-

import sys
import cmd
import sqlite3
import csv

import unicode_csv

class SQLiteConsole(cmd.Cmd):

  prompt = "=> "

  def __init__(self, database, *args, **kwargs):
    self.db_cur = database.cursor()
    self._stop = False
    self._changed = False
    cmd.Cmd.__init__(self, *args, **kwargs)

  @property
  def changed(self):
    return self._changed

  def default(self, query):
    if query.endswith('EOF'):
      self._stop = True
      return
    try:
      self.db_cur.execute(query)
    except sqlite3.OperationalError, e:
      print >> sys.stderr, e
      return
    if self.db_cur.description is not None:
      header = [col[0] for col in self.db_cur.description]
      writer = unicode_csv.Writer(sys.stdout)
      writer.writerow(header)
      for row in self.db_cur:
        writer.writerow(row)
    else:
      self._changed = True

  def emptyline(self):
    self._stop = True

  def postcmd(self, stop, line):
    return self._stop

  def postloop(self):
    print
