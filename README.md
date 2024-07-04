# DuckDump

An attempt to create a generic tool for extracting data from a DuckDB database, specifically focusing on records modified within the last X days.

## Usage

```sh
$ python3 dump.py -h
usage: dump.py [-h] --db DB [--days DAYS]

Dump data from a DuckDB database.

options:
  -h, --help   show this help message and exit
  --db DB      Name of the DuckDB database file
  --days DAYS  Number of days to look back from today
```

## Setup

```sh
python3 -m venv .venv
source .venv/bin/activate
```

```sh
pip install -r requirements.txt
```
