#!/usr/bin/env python3

import argparse
import logging
import time
import urllib.error
import urllib.request
from http.client import RemoteDisconnected, HTTPException
from threading import Thread

import utils
from db import DatabaseAdapter

WORKER_LOG_FORMAT = "%(asctime)s worker-%(process)d %(levelname)s: %(message)s"

GRACE_PERIOD = 30  # seconds


class ThreadedWorker(Thread):
    # def __init__(self, worker_id: int, database: DatabaseAdapter):
    def __init__(self, worker_id: int):
        super().__init__()
        self.name = f"Worker—{worker_id}"
        self.database = DatabaseAdapter()

    def run(self) -> None:
        logging.debug(f"{self.name} Enter")
        # with self.database.connect():
        logging.debug(f"{self.name} with connect")
        try:
            logging.debug(f"{self.name} try 01")
            url_id, url = self.database.get_next_url()
            logging.debug(f"{self.name} try 02")
            logging.debug(f"{self.name} — run {url_id}: {url}")
            logging.debug(f"{self.name} try 03")
            while url is not None:
                started = self.database.start_url_processing(url_id)
                if not started:
                    logging.debug(
                        f"{self.name}: {url_id}: '{url}' already processed by other worker"
                    )
                    url_id, url = self.database.get_next_url()
                    continue

                get_url(
                    url_id=url_id,
                    url=url,
                    database=self.database,
                    prefix=self.name
                )
                url_id, url = self.database.get_next_url()

        except Exception as e:
            logging.debug(f"{self.name} got and exception {e}")
        logging.debug(f"{self.name} is Done")


def get_url(url_id: int, url: str, database: DatabaseAdapter, prefix: str = None):
    def _debug(message: str):
        if prefix:
            message = f"{prefix} {message}"
        logging.debug(message)

    try:
        start = time.time()
        response = urllib.request.urlopen(url)
        end = time.time()
        _debug(f"Got {url_id}: '{url}' with {response.code} code in {end - start:.2f}s")
        database.mark_url_done(url_id, response.code)
    except urllib.error.URLError as e:
        _debug(f"Failed to fetch {url_id}: '{url}' because of {e.reason}")
        database.mark_url_error(url_id)
    except RemoteDisconnected as e:
        _debug(
            f"Failed to fetch {url_id}: '{url}' because remote "
            f"resource disconnected. Reason {e}")
        database.mark_url_error(url_id)
    except HTTPException as e:
        _debug(f"Failed to fetch {url_id}: '{url}' Got a HTTPException. Reason {e}")
        database.mark_url_error(url_id)
    except TimeoutError as e:
        _debug(f"Failed to fetch {url_id}: '{url}' Because of TimeoutError. Reason {e}")
        database.mark_url_error(url_id)


def worker():
    database = DatabaseAdapter()
    while True:
        url_id, url = database.get_next_url()
        if url is None:
            logging.info(f"No urls to process. Sleeping for {GRACE_PERIOD}s")
            time.sleep(GRACE_PERIOD)
            continue

        started = database.start_url_processing(url_id)
        if not started:
            logging.debug(f"{url_id}: '{url}' already processed by other worker")
            continue

        get_url(
            url_id=url_id,
            url=url,
            database=database,
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Worker.")
    parser.add_argument("-d", "--debug", help="Enable debug logging in workers",
                        action="store_true")
    args = parser.parse_args()
    utils.configure_logger(
        debug=args.debug,
        log_format=WORKER_LOG_FORMAT,
    )

    worker()
