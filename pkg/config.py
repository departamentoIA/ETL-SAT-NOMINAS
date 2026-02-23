import os
import urllib.parse
from dotenv import load_dotenv

load_dotenv()


def get_connection_string():
    """Create connection string."""
    SERVER = os.getenv('DB_SERVER')
    DATABASE = os.getenv('DB_NAME')
    USER = os.getenv('DB_USER')
    PASSWORD = os.getenv('DB_PASSWORD')
    if not all([SERVER, DATABASE, USER, PASSWORD]):
        raise ValueError("Faltan variables en el .env")

    params = urllib.parse.quote_plus(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD}'
    )
    return f"mssql+pyodbc:///?odbc_connect={params}"
