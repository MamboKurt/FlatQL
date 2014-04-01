# -*- coding: utf-8 -*-

import os
import argparse

class existing_directory(argparse.Action):
  def __call__(self,parser, namespace, values, option_string=None):
    prospective_path=os.path.abspath(values)
    if not os.path.exists(prospective_path):
      raise argparse.ArgumentTypeError(
        "{0} is not a valid path".format(prospective_path))
    if not os.path.isdir(prospective_path):
      raise argparse.ArgumentTypeError(
        "{0} is not a directory".format(prospective_path))
    else:
      setattr(namespace,self.dest,prospective_path)

class existing_file(argparse.Action):
  def __call__(self,parser, namespace, values, option_string=None):
    prospective_path=os.path.abspath(values)
    if not os.path.exists(prospective_path):
      raise argparse.ArgumentTypeError(
        "{0} is not a valid path".format(prospective_path))
    if not os.path.isfile(prospective_path):
      raise argparse.ArgumentTypeError(
        "{0} is not a file".format(prospective_path))
    else:
      setattr(namespace,self.dest,prospective_path)
