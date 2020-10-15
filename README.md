# Distributed python workers


Distributed python worker is a simple tool for internet resources status examining. It's written with vanilla Python and has no 3rd party dependencies.

"Worker" uses the SQLite database for queueing and synchronisation.

"Worker" implements two approaches: thread-based and process-based.

Due to SQLite3 nature, it's highly recommended to use the thread-based version only. The process-based is added for further extension and "bigger" database, like Postgres, support.

### Installation

1. Clone this repo
1. Ensure you have python3 installed in your system

"Worker" is tested in Linux and OSX.

### Usage

`./manage.py` is the main entry point.

optional arguments:
* `-h`, `--help` — show this help message and exit
* `-s`, `--stop` — Stop all running process-based workers
* `-e`, `--erase` — Erase database.
* `-l`, `--load` `%filename%` — Path to a file with URLs to load into the database
* `-w` , `--workers` `%count%` — Start given number process-based  of workers in the background
* `-t`, `--threads` `%count%` — Start given number of threaded workers
* `-d`, `--debug`           Enable debug logging in workers

The above flags could be used simultaneously. 
However, actions would be
executed in a certain order:

0. `stop`. Stop all running process-based workers
1. `erase` 
2. `load` 
3. `threads` & `debug` or `workers` & `debug`

**sample.txt** contains domains from https://moz.com/top500 
to use them for testing.

#### An example of running

```bash
> ./manage.py -l ./sample.txt
> ./manage.py -t 10 -d
```

In the other terminal session

```bash
> tail -f workers.log
```

#### Run verification

If you have `sqlite3` you could do the following to see database content.

```bash
> sqlite3 urls.sqlite3
sqlite> select * from urls;
```
