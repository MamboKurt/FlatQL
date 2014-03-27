import sys
import cmd
import csv

class SQLiteConsole(cmd.Cmd):

  prompt = "=> "

  def __init__(self, database, *args, **kwargs):
    self.db_cur = database.cursor()
    self._stop = False
    cmd.Cmd.__init__(self, *args, **kwargs)

  def default(self, query):
    if query.endswith('EOF'):
      self._stop = True
      return
    try:
      self.db_cur.execute(query)
    except sqlite3.OperationalError, e:
      print >> sys.stderr, e
      return
    header = [col[0] for col in self.db_cur.description]
    writer = csv.writer(sys.stdout)
    writer.writerow(header)
    for row in self.db_cur:
      writer.writerow(row)

  def emptyline(self):
    self._stop = True

  def postcmd(self, stop, line):
    return self._stop

  def postloop(self):
    print
