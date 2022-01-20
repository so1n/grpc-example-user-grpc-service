from functools import wraps
from typing import Any, Callable

import pymysql  # type: ignore
from dbutils.pooled_db import PooledDB  # type: ignore
from dbutils.steady_db import SteadyDBConnection  # type: ignore
from grpc_example_common.helper.context import context_proxy
from pymysql import cursors

# Need to use configuration management when producing the system
g_db_pool: PooledDB = PooledDB(
    pymysql,
    1,
    user="root",
    passwd="123123",
    blocking=True,
    db="grpc_example",
    cursorclass=cursors.DictCursor,
    charset="utf8",
)


def conn_proxy(auto_commit: bool = True) -> Callable:
    """负责db_util.getConnection的代理,执行成功自动commit,执行失败自动rollback并抛错
    >>> @conn_proxy()
    >>> def test():
    >>>     with context_proxy.conn.cursor() as cursor:
    >>>         cursor.execute("SHOW TABLES")
    """

    def wrapper(func: Callable) -> Callable:
        @wraps(func)
        def share_conn(*args: Any, **kwargs: Any) -> Any:
            db_conn: SteadyDBConnection = g_db_pool.connection()
            context_proxy.conn = db_conn
            try:
                response: Any = func(*args, **kwargs)
                if auto_commit:
                    db_conn.commit()
                return response
            except Exception as e:
                if auto_commit:
                    db_conn.rollback()
                raise e
            finally:
                db_conn.close()

        return share_conn

    return wrapper
