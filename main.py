#!/usr/bin/env python3

import argparse
import logging
import sqlite3
import time
import urllib.error
import urllib.request
from logging import handlers

DB_NAME = "./test.sqlite3"
SAMPLE_FILE = "./sample.txt"

STATUS_NEW = "NEW"
STATUS_PROCESSING = "PROCESSING"
STATUS_DONE = "DONE"
STATUS_ERROR = "ERROR"

LOG_FILE = "log.log"

LOG_FORMAT = "%(asctime)s worker-%(process)d %(levelname)s:  %(message)s"
LOG_LEVEL = 10


# CRITICAL = 50
# FATAL = CRITICAL
# ERROR = 40
# WARNING = 30
# WARN = WARNING
# INFO = 20
# DEBUG = 10
# NOTSET = 0

def main():
    logging.basicConfig(
        format=LOG_FORMAT,
        level=LOG_LEVEL,
        handlers=[handlers.WatchedFileHandler(filename=LOG_FILE)],
    )

    conn = sqlite3.connect(DB_NAME)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS urls (
            id integer PRIMARY KEY AUTOINCREMENT,
            url text NOT NULL,
            status text NOT NULL,
            http_code integer
        );
        """
    )
    print("Hello world")
    logging.debug("Hello world")

    counter = 3
    with open(SAMPLE_FILE) as fp:
        for line in fp:
            line = line.strip()
            conn.execute("INSERT INTO urls (url, status) VALUES (?,?)", (line, STATUS_NEW))
            counter -= 1
            if counter < 0:
                break
    conn.commit()

    cursor = conn.cursor()

    while True:
        cursor.execute("SELECT * FROM urls WHERE status=?", (STATUS_NEW,))
        url = cursor.fetchone()
        if url is None:
            break
        print(url)
        logging.debug(url)

        cursor.execute("UPDATE urls SET status=? WHERE id=? AND status=?",
                       (STATUS_PROCESSING, url[0], STATUS_NEW))
        if cursor.rowcount < 1:
            continue

        address = url[1]
        if not address.startswith("http"):
            address = f"https://{address}"
        try:
            start = time.time()
            response = urllib.request.urlopen(address)
            end = time.time()
            delta = end - start
            print(f"{address} — {response.code} in {delta:.2f}s")
            logging.debug(f"{address} — {response.code}")
            cursor.execute("UPDATE urls SET status=?, http_code=? WHERE id=?",
                           (STATUS_DONE, response.code, url[0]))
            conn.commit()
        except urllib.error.URLError as e:
            print(f"{address} — {e.reason}")
            logging.debug(f"{address} — {e.reason}")
            cursor.execute("UPDATE urls SET status=? WHERE id=?", (STATUS_ERROR, url[0]))
            conn.commit()

    u = cursor.execute("SELECT * FROM urls")
    u = cursor.fetchone()
    while u is not None:
        print(u)
        logging.debug(u)
        u = cursor.fetchone()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Workers manager.",
        epilog="""
        The above flags could be used simultaneously.
        However, actions would be executed in the certain order: 
        0 — `stop`
        1 — `erase`
        2 — `load`
        3 — `workers` & `debug`
        """)
    parser.add_argument("-s", "--stop", help="Stop all running workers", action="store_true")
    parser.add_argument("-e", "--erase", help="Erase database.", action="store_true")
    parser.add_argument("-l", "--load", help="Path to a file with urls to load into database",
                        type=str)
    parser.add_argument("-w", "--workers", help="Start given number of workers", type=int)
    parser.add_argument("-d", "--debug", help="Enable debug logging in workers",
                        action="store_true")
    args = parser.parse_args()
    print(f"args: {args}")
    # main()
