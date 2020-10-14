#!/usr/bin/env python3

import argparse
from subprocess import Popen, getoutput
import os
from os import path
import signal

import utils
from db import DatabaseAdapter

WORKER_PIDS = ".pids"
WORKER = "worker.py"

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
                continue
            print(f"Stopping worker process: {pid}")
            outputs = getoutput(f"ps -p {pid}")
            print(f"outputs {outputs}")
            if WORKER in outputs:
                os.kill(pid, signal.SIGTERM)
                count += 1
    # os.remove(pid_file)
    print(f"Stopped {count} workers")

def erase_database():
    print("Erasing database")
    database = DatabaseAdapter()
    database.drop_urls_table()


def load_urls_to_database(urls_file: str):
    print(f"Loading {urls_file} into database")


def start_workers(count: int, debug: bool):
    print(f"Starting {count} workers")
    worker_cmd = [path.join(utils.project_dir(), WORKER)]
    if debug:
        worker_cmd.append("-d")
    pids = list()
    for i in range(count):
        print(f"Starting worker: {i}")
        pid = Popen(worker_cmd).pid
        pids.append(pid)
        print(f"Started worker: {i} — {pid}")

    with open(get_pid_file(), "at") as fp:
        for pid in pids:
            fp.write(f"{pid}\n")


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

    if args.stop:
        kill_workers()

    if args.erase:
        erase_database()

    if args.load:
        load_urls_to_database(args.load)

    if args.workers:
        start_workers(args.workers, args.debug)
