import psycopg2
from utils.config import load_config

class DatabaseConnection:
    def __init__(self):
        self.config = load_config()

    def connect(self):
        """Connect to PostgreSQL database."""
        return psycopg2.connect(
            host=self.config['DB_HOST'],
            port=self.config['DB_PORT'],
            dbname=self.config['DB_NAME'],
            user=self.config['DB_USER'],
            password=self.config['DB_PASSWORD']
        )