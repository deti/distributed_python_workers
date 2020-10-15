#!/usr/bin/env python3

import argparse
import logging
import time
import urllib.error
import urllib.request
from http.client import RemoteDisconnected, HTTPException

import utils
from db import DatabaseAdapter

WORKER_LOG_FORMAT = "%(asctime)s worker-%(process)d %(levelname)s: %(message)s"

GRACE_PERIOD = 30  # seconds


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
        except RemoteDisconnected as e:
            logging.debug(f"Failed to fetch {url_id}: '{url}' because remote resource disconnected. Reason {e}")
            database.mark_url_error(url_id)
        except HTTPException as e:
            logging.debug(f"Failed to fetch {url_id}: '{url}' Got a HTTPException. Reason {e}")
            database.mark_url_error(url_id)


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
