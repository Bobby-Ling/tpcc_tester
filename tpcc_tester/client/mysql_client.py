from pathlib import Path
from typing import override
import pymysql

from .base import DBClient
from tpcc_tester.common import ServerState, Result

class MySQLClient(DBClient):
    def __init__(self, db: str = "tpcc_test", port: int = 3306, host: str = "localhost",
                 user: str = "root", password: str = "123123"):
        super().__init__(db, port)
        self.host = host
        self.user = user
        self.password = password
        self.connection = None

    @override
    def connect(self) -> ServerState:
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
                charset='utf8mb4',
                autocommit=True
            )

            self.connection.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {self.db};")
            self.connection.cursor().execute(f"USE {self.db};")

            self.logger.debug(f"Connected to MySQL at {self.host}:{self.port}/{self.db}")

            return ServerState.OK

        except Exception as e:
            self.logger.error(f"Failed to connect to MySQL: {e}")
            return ServerState.DOWN

    @DBClient.log_record
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

            result_str = self._format_result(raw_data)
            result = Result(ServerState.OK, meta_data, data, result_str, raw_data)
            self.logger.debug("exec sql: %s, result: %s", sql, result)
            return result
        except Exception as e:
            self.logger.error(f"Error executing SQL: {e}")
            return Result(ServerState.ABORT, [], [], str(e))

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