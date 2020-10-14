import sqlite3
from os import path
import utils
DB_NAME = "./urls.sqlite3"

STATUS_NEW = "NEW"
STATUS_PROCESSING = "PROCESSING"
STATUS_DONE = "DONE"
STATUS_ERROR = "ERROR"


class DatabaseAdapter:
    def __init__(self):
        db_file = path.join(utils.project_dir(), DB_NAME)
        self.conn = sqlite3.connect(db_file)

        # Highly arguable decision to make a migration on every class establishing
        # However it simplifies a little some scenarios
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

        self.cursor = self.conn.cursor()

    def drop_urls_table(self):
        """
        Clean up URLs table
        """
        self.conn.execute(
            """
            DROP TABLE IF EXISTS urls;
            """
        )

    def load_urls(self, urls):
        pass

    def insert_url(self, url: str):
        """
        Insert given url into Database
        """
        self.conn.execute(
            "INSERT INTO urls (url, status) VALUES (?,?)",
            (url, STATUS_NEW),
        )
        self.conn.commit()

    def get_next_url(self) -> (str, int):
        """
        Give next url and id to process. Returns tuple of None if no urls found
        """
        self.cursor.execute("SELECT * FROM urls WHERE status=?", (STATUS_NEW,))
        url = self.cursor.fetchone()
        if url is None:
            return None, None
        return url[0], url[1]

    def start_url_processing(self, url_id: int) -> bool:
        """
        Receives url_id which we are going to start processing.
        Returns True if processing started successfully.
        """
        self.cursor.execute(
            "UPDATE urls SET status=? WHERE id=? AND status=?",
            (STATUS_PROCESSING, url_id, STATUS_NEW),
        )
        self.conn.commit()
        if self.cursor.rowcount < 1:
            return False
        return True

    def mark_url_done(self, url_id, http_code: int):
        """
        Mark given url id as an Done, and save code of http response
        """
        self.cursor.execute(
            "UPDATE urls SET status=?, http_code=? WHERE id=?",
            (STATUS_DONE, http_code, url_id)
        )
        self.conn.commit()

    def mark_url_error(self, url_id: int):
        """
        Mark given url id as an Error
        """
        self.cursor.execute(
            "UPDATE urls SET status=? WHERE id=?",
            (STATUS_ERROR, url_id)
        )
        self.conn.commit()
