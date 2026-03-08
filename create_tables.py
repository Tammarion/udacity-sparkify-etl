import os
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def get_connection(dbname=None):
    """Connect to a PostgreSQL database using environment variables."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        dbname=dbname or os.getenv("DB_NAME", "sparkifydb"),
        user=os.getenv("DB_USER", "student"),
        password=os.getenv("DB_PASSWORD", "student")
    )


def create_database():
    """Create and connect to sparkifydb, return cursor and connection."""
    conn = get_connection(dbname=os.getenv("DB_DEFAULT", "studentdb"))
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    conn.close()

    conn = get_connection()
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """Drop each table using the queries in drop_table_queries."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create each table using the queries in create_table_queries."""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Drop and recreate all tables in sparkifydb."""
    cur, conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()
