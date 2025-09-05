import threading
from utils.db import DatabaseConnection

class DBWriter:
    def __init__(self, data_queue):
        self.data_queue = data_queue
        self.db = DatabaseConnection()

    def run(self):
        """Run the DB writer thread."""
        conn = self.db.connect()
        cur = conn.cursor()
        while True:
            data = self.data_queue.get()
            if data is None:
                break
            self.insert(cur, data)
            conn.commit()
        cur.close()
        conn.close()

    def insert(self, cur, data):
        """Insert data into the database."""
        insert_query = """
            INSERT INTO products (name, category, price_range, description)
            VALUES (%s, %s, %s, %s)
        """
        cur.execute(insert_query, (data['name'], data['category'], data['price_range'], data['description']))