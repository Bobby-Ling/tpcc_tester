import logging
from enum import Enum
from abc import ABC, abstractmethod
from typing import Callable

from tpcc_tester.common import ServerState, Result, setup_logging

# Operator
ALL = '*'
SELECT = 'select'
FROM = 'from'
WHERE = 'where'
AND = ' and '
ORDER_BY = 'order by'
DESC = 'desc'
ASC = 'asc'
INSERT = 'insert'
VALUES = 'values'
UPDATE = 'update'
DELETE = 'delete'
SET = 'set'
EQ = '='
GT = '>'
LT = '<'
GE = '>='
LE = '<='

class ClientType(Enum):
    RMDB = 'rmdb'
    MYSQL = 'mysql'
    SLT = 'slt'
    SQL = 'sql'

class DBClient(ABC):
    @staticmethod
    def from_type(client_type: ClientType):
        from tpcc_tester.client.rmdb_client import RMDBClient
        from tpcc_tester.client.mysql_client import MySQLClient
        from tpcc_tester.client.slt_client import SLTClient
        from tpcc_tester.client.sql_client import SQLClient

        if client_type == ClientType.RMDB:
            return RMDBClient()
        elif client_type == ClientType.MYSQL:
            return MySQLClient()
        elif client_type == ClientType.SLT:
            return SLTClient()
        elif client_type == ClientType.SQL:
            return SQLClient()
        else:
            raise ValueError(f'Invalid client type: {client_type}')

    def __init__(self, db: str, port: int):
        self.db = db
        self.port = port
        self.logger = setup_logging(__name__)
        #
        self.sql_logger = setup_logging(
            self.logger.name + '_sql',
            console_level=logging.ERROR,
            file_level=logging.DEBUG,
            console_formatter='%(message)s',
            file_formatter='%(message)s',
            log_file=f'{self.logger.name}.log.sql'
        )

    @abstractmethod
    def connect(self) -> ServerState:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    def crash(self) -> None:
        pass

    @staticmethod
    def log_record(func: Callable[..., Result]):
        def wrapper(self: 'DBClient', *args, **kwargs):
            result = func(self, *args, **kwargs)
            self.append_record(args[0], result)
            return result
        return wrapper

    def append_record(self, sql: str, result: Result) -> None:
        self.sql_logger.info(f"{sql}")
        self.sql_logger.info(f"{'\n'.join([f'-- {line}' for line in result.result_str.split('\n') if line])}\n")
        if result.state == ServerState.ERROR:
            raise Exception(result.result_str)
        # 记录sql
        result.sql = sql

        # self.sql_logger.info("-" * 50)

    @abstractmethod
    @log_record
    def send_cmd(self, sql: str) -> Result:
        pass

    def send_ddl(self, sql: str) -> Result:
        return self.send_cmd(sql)

    def send_dml(self, sql: str) -> Result:
        return self.send_cmd(sql)

    def send_dql(self, sql: str) -> Result:
        return self.send_cmd(sql)

    def send_tcl(self, sql: str) -> Result:
        return self.send_cmd(sql)

    def begin(self) -> Result:
        return self.send_tcl("BEGIN;")

    def commit(self) -> Result:
        return self.send_tcl("COMMIT;")

    def abort(self) -> Result:
        return self.send_tcl("ABORT;")

    def select(self, table, col=ALL, where=False, order_by=False, asc=False):
        if type(table) != list:
            table = [table]
        # if type(col) != list:
        #     col = [col]
        if where and type(where) != list:
            where = [where]

        param = []
        if where:
            param = [ele[-1] for ele in where]

        table = ','.join(table)

        gen = lambda ele: str(ele[0]) + str(ele[1]) + '%s'
        where = ' '.join([WHERE, AND.join([gen(ele) for ele in where])]) if where else ''
        order_by = ' '.join([ORDER_BY, order_by, ASC if asc else DESC]) if order_by else ''
        sql = ' '.join([SELECT, ','.join(col), FROM, table, where, order_by, ';'])
        for i in param:
            sql = sql.replace("%s", str(i), 1)

        return self.send_dql(sql)


    def insert(self, table, rows):
        values = ''.join([VALUES, '(', ','.join(['%s' for i in range(len(rows))]), ')'])
        sql = ' '.join([INSERT, "into", table, values, ';'])
        for i in rows:
            sql = sql.replace("%s", str(i), 1)
        return self.send_dml(sql)


    def update(self, table, row, where=False):
        if type(row) != list:
            row = [row]
        if type(where) != list:
            where = [where]
        param = [ele[-1] for ele in where]
        gen = lambda ele: str(ele[0]) + str(ele[1]) + '%s'
        where = ' '.join([WHERE, AND.join([gen(ele) for ele in where])]) if where else ''
        var = [e[0] + '=%s' for e in row]
        val = [e[1] for e in row]

        sql = ' '.join([UPDATE, table, SET, ','.join(var), where, ';'])
        for i in val:
            sql = sql.replace("%s", str(i), 1)
        for i in param:
            sql = sql.replace("%s", str(i), 1)
        return self.send_dml(sql)

    def delete(self, table, where):
        if type(where) != list:
            where = [where]
        param = [ele[-1] for ele in where]
        gen = lambda ele: str(ele[0]) + str(ele[1]) + '%s'
        where = ' '.join([WHERE, AND.join([gen(ele) for ele in where])]) if where else ''
        sql = ' '.join([DELETE, FROM, table, where, ';'])
        for i in param:
            sql = sql.replace("%s", str(i), 1)
        return self.send_dml(sql)

    # def create_table(self, sql: str) -> Result:
    #     return self.send_ddl(sql)

    # def create_index(self, sql: str) -> Result:
    #     return self.send_ddl(sql)

    # def drop_table(self, sql: str) -> Result:
    #     return self.send_ddl(sql)

    # def drop_index(self, sql: str) -> Result:
    #     return self.send_ddl(sql)

    # def select(self, sql: str) -> Result:
    #     return self.send_dql(sql)

    # def insert(self, sql: str) -> Result:
    #     return self.send_dml(sql)

    # def update(self, sql: str) -> Result:
    #     return self.send_dml(sql)

    # def delete(self, sql: str) -> Result:
    #     return self.send_dml(sql)