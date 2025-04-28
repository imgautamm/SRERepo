# db.py
import os
import psycopg2

def get_connection():
    """Establish a database connection using environment variables."""
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            database=os.getenv('POSTGRES_DB', 'my_db'),
            user=os.getenv('POSTGRES_USER', 'my_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'my_password')
        )
        return conn
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        raise
