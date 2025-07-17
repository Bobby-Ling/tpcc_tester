from pathlib import Path

from typing import override

from .base import DBClient
from tpcc_tester.common import ServerState, Result


class SQLClient(DBClient):
    def __init__(self, db: str = "test", port: int = 0, sql_file: str = "test.sql"):
        super().__init__(db, port)
        self.sql_file = Path(sql_file)
        self.sql_file.parent.mkdir(parents=True, exist_ok=True)
        self.f = open(self.sql_file, 'w')

        self.f.write("-- SQL file generated\n\n")

    @override
    def connect(self) -> ServerState:
        return ServerState.OK

    @override
    def send_cmd(self, sql: str) -> Result:
        self.f.write(f"{sql}\n")
        return Result(ServerState.OK, [], "Recorded to SQL file")

    @override
    def close(self):
        self.f.close()
        self.logger.debug(f"SQL file saved to {self.sql_file}")