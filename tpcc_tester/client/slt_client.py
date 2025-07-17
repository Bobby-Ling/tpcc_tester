from pathlib import Path

from typing import override

from .base import DBClient
from tpcc_tester.common import ServerState, Result

class SLTClient(DBClient):
    def __init__(self, db: str = "test", port: int = 0, slt_file: str = "test.slt"):
        super().__init__(db, port)
        self.slt_file = Path(slt_file)
        self.slt_file.parent.mkdir(parents=True, exist_ok=True)
        self.f = open(self.slt_file, 'w')

        self.f.write("# SQL Logic Test file generated\n\n")

    @override
    def connect(self) -> ServerState:
        return ServerState.OK

    @override
    def send_cmd(self, sql: str) -> Result:
        return Result(ServerState.OK, [], "Recorded to SLT file")

    def _send_query(self, sql: str) -> Result:
        self.f.write(f"query T\n{sql}\n----\n\n")
        return self.send_cmd(sql)

    def _send_statement(self, sql: str) -> Result:
        self.f.write(f"statement ok\n{sql}\n\n")
        return self.send_cmd(sql)

    @override
    def send_dql(self, sql: str) -> Result:
        return self._send_query(sql)

    @override
    def send_dml(self, sql: str) -> Result:
        return self._send_statement(sql)

    @override
    def send_tcl(self, sql: str) -> Result:
        return self._send_statement(sql)

    @override
    def close(self):
        self.f.close()
        self.logger.debug(f"SLT file saved to {self.slt_file}")

