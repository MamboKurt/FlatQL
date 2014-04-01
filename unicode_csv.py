# -*- coding: utf-8 -*-

import csv

class Reader(object):
    def __init__(self, *args, **kwargs):
        self.encoding = kwargs.pop('encoding', 'utf8')
        self.reader = csv.reader(*args, **kwargs)

    def next(self):
        return [unicode(c, self.encoding) for c in self.reader.next()]

    def __iter__(self):
        return self

    @property
    def dialect(self):
        return self.reader.dialect

    @property
    def line_num(self):
        return self.reader.line_num


class Writer(object):
    def __init__(self, *args, **kwargs):
        self.encoding = kwargs.pop('encoding', 'utf8')
        self.writer = csv.writer(*args, **kwargs)

    def writerow(self, row):
        self.writer.writerow([unicode(column).encode(self.encoding) for column in row])

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)