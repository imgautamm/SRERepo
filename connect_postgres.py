import psycopg2

def connect_to_postgres():
    try:
        connection = psycopg2.connect(
            host="localhost",
            port=5432,
            database="my_db",
            user="my_user",
            password="my_password"
        )
        cursor = connection.cursor()
        cursor.execute('SELECT version();')
        db_version = cursor.fetchone()
        print(f"Connected to Postgres! Version: {db_version}")

        cursor.close()
        connection.close()

    except Exception as error:
        print(f"Error connecting to Postgres: {error}")

if __name__ == "__main__":
    connect_to_postgres()
