import sqlite3
import urllib.request
import urllib.error

DB_NAME = "./test.sqlite3"
SAMPLE_FILE = "./sample.txt"

STATUS_NEW = "NEW"
STATUS_PROCESSING = "PROCESSING"
STATUS_DONE = "DONE"
STATUS_ERROR = "ERROR"

def main():
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

        cursor.execute("UPDATE urls SET status=? WHERE id=? AND status=?", (STATUS_PROCESSING, url[0], STATUS_NEW))
        if cursor.rowcount < 1:
            continue

        address = url[1]
        if not address.startswith("http"):
            address = f"https://{address}"
        try:
            response = urllib.request.urlopen(address)
            print(f"{address} — {response.code}")
            cursor.execute("UPDATE urls SET status=?, http_code=? WHERE id=?", (STATUS_DONE, response.code, url[0]))
            conn.commit()
        except urllib.error.URLError as e:
            print(f"{address} — {e.reason}")
            cursor.execute("UPDATE urls SET status=? WHERE id=?", (STATUS_ERROR, url[0]))
            conn.commit()

    u = cursor.execute("SELECT * FROM urls")
    u = cursor.fetchone()
    while u is not None:
        print(u)
        u = cursor.fetchone()

if __name__ == '__main__':
    main()