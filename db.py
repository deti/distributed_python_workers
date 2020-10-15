import sqlite3
from os import path
import utils
from contextlib import contextmanager

DB_NAME = "./urls.sqlite3"
DB_TIMEOUT = 20

STATUS_NEW = "NEW"
STATUS_PROCESSING = "PROCESSING"
STATUS_DONE = "DONE"
STATUS_ERROR = "ERROR"


class DatabaseAdapter:
    def __init__(self):
        self.db_file = path.join(utils.project_dir(), DB_NAME)

    def create_urls_table(self):
        """
        Create URLs table
        """
        with self.connect():
            with self.commit():
                self.conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS urls (
                        id integer PRIMARY KEY AUTOINCREMENT,
                        url text NOT NULL,
                        status text NOT NULL,
                        http_code integer
                    );
                    """
                )

    def drop_urls_table(self):
        """
        Clean up URLs table
        """
        with self.connect():
            self.conn.execute(
                """
           DROP TABLE IF EXISTS urls;
                """
            )

    @contextmanager
    def connect(self):
        self.conn = sqlite3.connect(self.db_file, timeout=DB_TIMEOUT, check_same_thread=False)
        yield
        self.conn.close()

    @contextmanager
    def commit(self):
        """
        Context manager to make multi inserts in to
        """
        yield
        self.conn.commit()


    def naked_insert_url(self, url:str):
        """
        Insert given url into Database without commit.
        Should be used with commit
        """
        self.conn.execute(
            "INSERT INTO urls (url, status) VALUES (?,?)",
            (url, STATUS_NEW),
        )

    def insert_url_with_commit(self, url: str):
        """
        Insert given url into Database and commits it
        """
        with self.connect():
            with self.commit():
                self.naked_insert_url(url)

    def get_next_url(self) -> (int, str):
        """
        Give next id and url to process. Returns tuple of None if no urls found
        """
        with self.connect():
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM urls WHERE status=?", (STATUS_NEW,))
            url = cursor.fetchone()
        if url is None:
            return None, None
        return url[0], url[1]

    def start_url_processing(self, url_id: int) -> bool:
        """
        Receives url_id which we are going to start processing.
        Returns True if processing started successfully.
        """
        with self.connect():
            with self.commit():
                cursor = self.conn.cursor()
                cursor.execute(
                    "UPDATE urls SET status=? WHERE id=? AND status=?",
                    (STATUS_PROCESSING, url_id, STATUS_NEW),
                )
                if cursor.rowcount < 1:
                    return False
        return True

    def mark_url_done(self, url_id, http_code: int):
        """
        Mark given url id as an Done, and save code of http response
        """
        with self.connect():
            with self.commit():
                cursor = self.conn.cursor()
                cursor.execute(
                    "UPDATE urls SET status=?, http_code=? WHERE id=?",
                    (STATUS_DONE, http_code, url_id)
                )

    def mark_url_error(self, url_id: int):
        """
        Mark given url id as an Error
        """
        with self.connect():
            with self.commit():
                cursor = self.conn.cursor()
                cursor.execute(
                    "UPDATE urls SET status=? WHERE id=?",
                    (STATUS_ERROR, url_id)
                )
