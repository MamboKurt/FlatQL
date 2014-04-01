# -*- coding: utf-8 -*-

import csv


DEFAULT_FILE_ENCODING = 'utf-8'

class Reader(object):
    def __init__(self, *args, **kwargs):
        self._encoding = kwargs.pop('encoding', DEFAULT_FILE_ENCODING)
        self._reader = csv.reader(*args, **kwargs)

    def next(self):
        return [unicode(c, self._encoding) for c in self._reader.next()]

    def __iter__(self):
        return self

    @property
    def dialect(self):
        return self._reader.dialect

    @property
    def line_num(self):
        return self._reader.line_num


class Writer(object):
    def __init__(self, *args, **kwargs):
        self._encoding = kwargs.pop('encoding', DEFAULT_FILE_ENCODING)
        self._writer = csv.writer(*args, **kwargs)

    def writerow(self, row):
        self._writer.writerow([unicode(column).encode(self._encoding) for column in row])

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)