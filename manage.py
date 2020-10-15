#!/usr/bin/env python3

import argparse
import logging
import os
import signal
from os import path
from subprocess import Popen, getoutput
import time

import utils
from db import DatabaseAdapter
from worker import ThreadedWorker

WORKER_PIDS = ".pids"
WORKER = "worker.py"

START_DELAY = 0.5

def _info(message, sender: str):
    print(message)
    logging.info(f"{sender} — {message}")


def get_pid_file():
    pid_file = path.join(utils.project_dir(), WORKER_PIDS)
    return pid_file


def kill_workers():
    pid_file = get_pid_file()
    if not path.exists(pid_file):
        print("Looks like there's no workers to stop. Please verify it manually")
        return
    count = 0
    with open(pid_file) as fp:
        for pid in fp:
            try:
                pid = int(pid)
            except ValueError:
                logging.debug(f"kill_workers could not parse pid `{pid}`")
                continue

            _info(f"Stopping worker process: {pid}", "kill_workers")
            outputs = getoutput(f"ps -p {pid}")
            if WORKER in outputs:
                os.kill(pid, signal.SIGTERM)
                count += 1
            else:
                logging.debug(
                    f"Looks like process {pid} isn't a worker"
                    f"`ps -p` result: `{outputs}`"
                )
    os.remove(pid_file)
    _info(f"Stopped {count} workers", "kill_workers")


def erase_database():
    database = DatabaseAdapter()
    database.drop_urls_table()
    _info("Database cleaned", "erase_database")


def load_urls_to_database(urls_file: str):
    _info(f"Loading `{urls_file}` into database", "load_urls_to_database")
    count = 0
    errors = 0
    database = DatabaseAdapter()

    if not path.isfile(urls_file):
        _info(f"Given path: `{urls_file}` is not a file", "load_urls_to_database")
        return

    with open(urls_file) as fp:
        database.create_urls_table()

        with database.connect():
            with database.commit():
                for url in fp:
                    url = url.strip()
                    database.naked_insert_url(url)
                    count += 1

    _info(f"Loaded {count} urls into database with {errors} errors", "load_urls_to_database")


def start_workers(count: int, debug: bool):
    _info(f"Starting {count} workers", "start_workers")
    worker_cmd = [path.join(utils.project_dir(), WORKER)]
    if debug:
        worker_cmd.append("-d")

    with open(get_pid_file(), "at") as fp:
        for i in range(count):
            pid = Popen(worker_cmd).pid
            _info(f"Started worker with pid {pid}", "start_workers")
            fp.write(f"{pid}\n")
            # SQLite doesn't allow multi process safe connection
            # This delay is for letting workers start safely
            time.sleep(START_DELAY)
            # In general this is a weak decision. That shouldn't be done this way in
            # the Production
            # On the other hand, in the production we would use better DBMS which
            # Could handle row-level locks

def start_threaded_workers(count: int=1):

    threads = [ThreadedWorker(i) for i in range(count)]

    for t in threads:
        time.sleep(0.3)
        t.start()

    print(f"Started {count} workers")

    for t in threads:
        print(f"start_threaded_workers: {t.name} — finished it's work")
        t.join()

    print(f"All {count} are done")

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
    parser.add_argument("-t", "--threads", help="Start given number of threaded workers", type=int)
    parser.add_argument("-d", "--debug", help="Enable debug logging in workers",
                        action="store_true")
    args = parser.parse_args()
    utils.configure_logger(args.debug)

    if args.workers is not None and args.threads is not None:
        print("You couldn't start both threaded and process based workers simultaneously")
        exit(1)

    if args.stop:
        kill_workers()

    if args.erase:
        erase_database()

    if args.load:
        load_urls_to_database(args.load)

    if args.threads:
        if args.threads < 1:
            print("Please let us start at least 1 thread")
            exit(2)
        if args.threads > 10:
            print(f"We let you start {args.threads} but please reconsider it")
        start_threaded_workers(args.threads)

    if args.workers:
        start_workers(args.workers, args.debug)
