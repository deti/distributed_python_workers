import logging
from logging import handlers
import sqlite3
import urllib.request
import urllib.error
from datetime import datetime
import time

DB_NAME = "./test.sqlite3"
SAMPLE_FILE = "./sample.txt"

STATUS_NEW = "NEW"
STATUS_PROCESSING = "PROCESSING"
STATUS_DONE = "DONE"
STATUS_ERROR = "ERROR"

LOG_FILE = "log.log"

LOG_FORMAT = "%(asctime)s worker-%(process)d %(levelname)s:  %(message)s"
LOG_LEVEL = 10
          #CRITICAL = 50
          #FATAL = CRITICAL
          #ERROR = 40
          #WARNING = 30
          #WARN = WARNING
          #INFO = 20
          #DEBUG = 10
          #NOTSET = 0

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

        cursor.execute("UPDATE urls SET status=? WHERE id=? AND status=?", (STATUS_PROCESSING, url[0], STATUS_NEW))
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
            cursor.execute("UPDATE urls SET status=?, http_code=? WHERE id=?", (STATUS_DONE, response.code, url[0]))
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
    main()