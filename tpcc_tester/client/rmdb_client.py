import os
from pathlib import Path
import socket
from typing import Any, List
from typing import override

from .base import DBClient
from tpcc_tester.common import ServerState, Result

class RMDBClient(DBClient):
    MAX_MEM_BUFFER_SIZE = 8192
    HOST = '127.0.0.1'

    def __init__(self, db: str = "rmdb", port: int = int(os.getenv("RMDB_PORT", "8765"))):
        super().__init__(db, port)
        self.socket = None

    @override
    def connect(self) -> ServerState:
        try:
            if self.socket:
                self.socket.close()

            host = socket.gethostbyname(self.HOST)
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((host, self.port))

            try:
                self.socket.sendall("show tables;".encode())
                recv_buf = self.socket.recv(self.MAX_MEM_BUFFER_SIZE)
                if recv_buf:
                    self.logger.debug(f"Connected to RMDB at {self.HOST}:{self.port}")
                    return ServerState.OK
                else:
                    return ServerState.DOWN
            except:
                return ServerState.DOWN

        except Exception as e:
            self.logger.error(f"Failed to connect to RMDB: {e}")
            return ServerState.DOWN

    @DBClient.log_record
    @override
    def send_cmd(self, sql: str) -> Result:
        try:
            self.socket.sendall(sql.encode())
            recv_buf = self.socket.recv(self.MAX_MEM_BUFFER_SIZE)

            if not recv_buf:
                self.logger.warning("Connection closed by server")
                return Result(ServerState.DOWN, [], [], "Connection closed")

            result_str = recv_buf.decode()

            # 解析结果状态
            if result_str.startswith('abort') or result_str.startswith('ABORT'):
                return Result(ServerState.ABORT, [], [], result_str)
            elif result_str.startswith('Error') or result_str.startswith('ERROR'):
                return Result(ServerState.ABORT, [], [], result_str)
            else:
                # 解析查询结果
                raw_data = self._parse_query_result(result_str)
                metadata = raw_data[0] if len(raw_data) > 0 else []
                data = raw_data[1:] if len(raw_data) > 1 else []
                return Result(ServerState.OK, metadata, data, result_str, raw_data)

        except Exception as e:
            self.logger.error(f"Error sending command: {e}")
            return Result(ServerState.ABORT, [], [], str(e))

    def _parse_query_result(self, result_str: str) -> List[List[Any]]:
        """解析查询结果字符串为结构化数据"""
        if not result_str or 'Error' in result_str:
            return []

        lines = result_str.strip().split('\n')
        data_rows = []

        # 跳过表头，解析数据行
        for line in lines:
            if line.startswith('|') and '|' in line[1:]:
                # 移除前后的'|'并分割
                row_data = [cell.strip() for cell in line.strip('|').split('|')]
                if row_data and any(cell for cell in row_data):  # 非空行
                    data_rows.append(row_data)

        return data_rows

    @override
    def close(self):
        if self.socket:
            try:
                self.socket.close()
                self.logger.debug("RMDB connection closed")
            except:
                pass
            finally:
                self.socket = None
