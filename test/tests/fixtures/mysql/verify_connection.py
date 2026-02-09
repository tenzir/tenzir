# runner: python
"""Verify the MySQL fixture starts correctly and has test data."""

import os

import pymysql

# /// script
# dependencies = ["pymysql"]
# ///


def main() -> None:
    """Connect to MySQL and verify test data exists."""
    conn = pymysql.connect(
        host=os.environ["MYSQL_HOST"],
        port=int(os.environ["MYSQL_PORT"]),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.environ["MYSQL_DATABASE"],
    )
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT name FROM users ORDER BY name;")
            for (name,) in cursor.fetchall():
                print(name)


if __name__ == "__main__":
    main()
