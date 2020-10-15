import logging
from os import path
from logging import handlers

LOG_FILE = "workers.log"
LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"

def project_dir():
    return path.dirname(path.realpath(__file__))

def configure_logger(debug: bool=False, log_format:str=None):
    log_file = path.join(project_dir(), LOG_FILE)

    if log_format is None:
        log_format = LOG_FORMAT

    log_level = 20  # INFO
    if debug:
        log_level = 10  # DEBUG

    logging.basicConfig(
        format=log_format,
        level=log_level,
        handlers=[handlers.WatchedFileHandler(filename=log_file)],
    )
