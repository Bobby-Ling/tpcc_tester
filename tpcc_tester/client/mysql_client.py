from pathlib import Path
from typing import override
import pymysql
from multiprocessing.synchronize import Lock as LockBase
from .base import DBClient
from tpcc_tester.common import ServerState, Result

class MySQLClient(DBClient):
    def __init__(self, db: str = "tpcc_test", port: int = 3306, host: str = "localhost",
                 user: str = "root", password: str = "123123", global_lock: LockBase = None):
        super().__init__(db, port, global_lock)
        self.host = host
        self.user = user
        self.password = password
        self.connection = None

    @override
    def connect(self) -> ServerState:
        try:
            # https://stackoverflow.com/questions/59871904/convert-pymysql-query-result-with-mysql-decimal-type-to-python-float
            conversions = pymysql.converters.conversions
            conversions[pymysql.FIELD_TYPE.NEWDECIMAL] = lambda x: float(x)
            conversions[pymysql.FIELD_TYPE.DECIMAL] = lambda x: float(x)
            conversions[pymysql.FIELD_TYPE.FLOAT] = lambda x: float(x)
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
                charset='utf8mb4',
                conv=conversions,
                autocommit=True,
                local_infile=True, # for load data local infile
            )

            # sudo vim /etc/mysql/mysql.conf.d/mysqld.cnf
            # apppend local_infile=1 in [mysqld] section
            # sudo systemctl restart mysql

            self.connection.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {self.db};")
            self.connection.cursor().execute(f"USE {self.db};")
            self.connection.cursor().execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            # self.connection.cursor().execute("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            self.connection.cursor().execute("SELECT @@transaction_isolation;")

            self.logger.debug(f"Connected to MySQL at {self.host}:{self.port}/{self.db}")

            return ServerState.OK

        except Exception as e:
            self.logger.error(f"Failed to connect to MySQL: {e}")
            return ServerState.DOWN

    @DBClient.log_record
    @DBClient.with_global_lock
    @override
    def send_cmd(self, sql: str) -> Result:
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            result_col_data = cursor.fetchall()
            column_names = tuple(column[0] for column in cursor.description) if cursor.description else ()
            raw_data = (column_names,) + tuple(result_col_data)

            meta_data = list(column_names)
            data = [list(row) for row in result_col_data]

            for row in data:
                for i, cell in enumerate(row):
                    if type(cell) == int:
                        row[i] = str(cell)
                    if type(cell) == float:
                        row[i] = f"{cell:.2f}"

            result_str = self._format_result(raw_data)
            result = Result(ServerState.OK, meta_data, data, result_str, raw_data, sql)
            # self.logger.debug("exec sql: %s, result: %s", sql, result)
            return result
        except Exception as e:
            self.logger.exception(f"Error executing SQL: {sql} error: {e}")
            return Result(ServerState.ERROR, [], [], str(e), e, sql)

    def _format_result(self, result_data) -> str:
        if not result_data:
            return ""

        lines = []
        for row in result_data:
            lines.append('|' + '|'.join(str(cell) for cell in row) + '|')

        return '\n'.join(lines)

    @override
    def crash(self) -> Result:
        """MySQL的crash相当于abort"""
        return self.abort()

    @override
    def close(self):
        try:
            self.connection.close()
            self.logger.debug("MySQL connection closed")
        except:
            pass
        finally:
            self.connection = None

    @override
    def abort(self) -> Result:
        self.send_tcl("ROLLBACK;")