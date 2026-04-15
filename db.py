import mysql.connector
from mysql.connector import errors as mysql_errors

from config import DB_CONFIG


def db_conn():
    return mysql.connector.connect(**DB_CONFIG)


def safe_fetch_one(cur, sql, params):
    """SELECT ... LIMIT 1 com tratamento; retorna dict ou None."""
    try:
        cur.execute(sql, params)
        return cur.fetchone()
    except (
        mysql_errors.ProgrammingError,
        mysql_errors.DatabaseError,
        mysql_errors.InterfaceError,
        mysql_errors.OperationalError,
    ):
        return None


def _get_db():
    return db_conn()
