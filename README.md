# FlatQL

FlatQL is a small tool for working on CSV files in a directory as it was
an SQLite database containing the files tables.

## Intro

FlatQL ist meant to be a small tool to work on a CSV textfile based,
persistent database.

It is small and simple and in some way stupid. It does not correct
errors in the CSV files or anything else. If it`s wrong it will print
an error.

It is also not ment to be fast! If you need a fast database, textbased
databases are probably not what you are looking for.

## How it works

### Usage

    usage: flatql.py [-h] [-p PATH] [-q QUERY]

    Execute SQL-Queries on a Folder containing CSV Files

    optional arguments:
      -h, --help            show this help message and exit
      -p PATH, --path PATH  Database Directory Path
      -q QUERY, --query QUERY
                            Semicolon separated SQLite Queries. None gives you an
                            SQLite Terminal

### What it does

FlatQL creates an SQLite database in the memory and reads all files with
suffix `csv` in a table in this database. The content of the file
`foobar.csv` will be put in the table `foobar`.
Once the tables are created you can query the table in the usual manner.

There are two modes:

* Interactive Mode
* Commandline Argment Mode

If you ommit the commandline argument `-q/--query` an interactive shell
will pop up. Here you can issue SQL queries.
If you give the argument `-q/--query`,
e.g. `-q "SELECT * FROM foo_table"` or
`-q "DELETE FROM foo_table WHERE bar = 5"` it will modify the table or
output the result of the query.
