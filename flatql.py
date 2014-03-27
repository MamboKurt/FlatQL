#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import argparse

from tools import existing_path


def main():
  argument_parser = argparse.ArgumentParser(
    description="Execute SQL-Queries on a Folder" +
      " containing CSV Files")
  argument_parser.add_argument('-p', '--path',
    default='./',
    action=existing_path,
    help="Database Directory Path")

  parsed_arguments = argument_parser.parse_args()
  

if __name__ == '__main__':
  sys.exit(main())