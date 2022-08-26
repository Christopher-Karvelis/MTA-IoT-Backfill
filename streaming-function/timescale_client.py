import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


class TimescaleClient:
    def __init__(self):
        self.password = os.getenv("TIMESCALE_PASSWORD")
        self.username = os.getenv("TIMESCALE_USERNAME")
        self.host = os.getenv("TIMESCALE_HOST_URL")
        self.port = os.getenv("TIMESCALE_PORT")
        self.dbname = os.getenv("TIMESCALE_DATABASE_NAME")
        self.connection = f"postgres://{self.username}:{self.password}@{self.host}:{self.port}/{self.dbname}"

    def get_connection(self):
        return psycopg2.connect(self.connection)
