import os

import mysql.connector
from mysql.connector import errors as mysql_errors
from mysql.connector import pooling

from config import DB_CONFIG


# Pool de conexões MySQL.
#
# Mantemos a assinatura pública de db_conn() igual à anterior para não exigir
# mudanças nas rotas/services. Quem chama continua fazendo:
#
#   conn = db_conn()
#   ...
#   conn.close()
#
# Com conexão vinda do pool, o close() devolve a conexão ao pool.
_POOL = None


def _pool_size():
    try:
        return max(1, int(os.getenv("DB_POOL_SIZE", "5")))
    except ValueError:
        return 5


def _pool_config():
    cfg = dict(DB_CONFIG)

    # Evita espera longa quando o banco estiver indisponível.
    cfg.setdefault("connection_timeout", int(os.getenv("DB_CONNECTION_TIMEOUT", "10")))

    return cfg


def _get_pool():
    global _POOL

    if _POOL is None:
        _POOL = pooling.MySQLConnectionPool(
            pool_name=os.getenv("DB_POOL_NAME", "ecc_pool"),
            pool_size=_pool_size(),
            pool_reset_session=True,
            **_pool_config(),
        )

    return _POOL


def db_conn():
    """Retorna uma conexão MySQL.

    A partir desta versão, a conexão vem de um pool. O restante do sistema não
    precisa mudar: ao chamar conn.close(), a conexão é devolvida ao pool.
    """
    return _get_pool().get_connection()


# Alias opcional para uso futuro, sem quebrar chamadas antigas.
def get_db_connection():
    return db_conn()


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
