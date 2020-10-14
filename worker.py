#!/usr/bin/env python3

import os
import argparse
import logging
import time
import urllib.error
import urllib.request
from logging import handlers
from os import path

import utils
from db import DatabaseAdapter

LOG_FILE = "workers.log"

LOG_FORMAT = "%(asctime)s worker-%(process)d %(levelname)s:  %(message)s"

GRACE_PERIOD = 30  # seconds


def configure_logger(debug: bool):
    log_file = path.join(utils.project_dir(), LOG_FILE)

    log_level = 20  # INFO
    if debug:
        log_level = 10  # DEBUG
    logging.basicConfig(
        format=LOG_FORMAT,
        level=log_level,
        handlers=[handlers.WatchedFileHandler(filename=log_file)],
    )


def worker(debug: bool):
    database = DatabaseAdapter()
    while True:
        url, url_id = database.get_next_url()
        if url is None:
            logging.info(f"No urls to process. Sleeping for {GRACE_PERIOD}s")
            time.sleep(GRACE_PERIOD)
            continue

        started = database.start_url_processing(url_id)
        if not started:
            logging.debug(f"{url_id}: '{url}' already processed by other worker")
            continue

        try:
            start = time.time()
            response = urllib.request.urlopen(url)
            end = time.time()
            logging.debug(
                f"Got {url_id}: '{url}' with {response.code} code in {end - start:.2f}s")
            database.mark_url_done(url_id, response.code)
        except urllib.error.URLError as e:
            logging.debug(f"Failed to fetch {url_id}: '{url}' because of {e.reason}")
            database.mark_url_error(url_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Worker.")
    parser.add_argument("-d", "--debug", help="Enable debug logging in workers",
                        action="store_true")
    args = parser.parse_args()
    configure_logger(args.debug)
    print(f"start worker {os.getpid()}")
    time.sleep(90)
    print(f"stop worker {os.getpid()}")

    # worker(debug=args.debug)
