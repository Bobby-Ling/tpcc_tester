import pathlib
from typing import override

file_path = pathlib.Path(__file__)
file_dir = file_path.parent
project_dir = file_dir.parent

from tpcc_tester.driver.tpcc_driver import TpccDriver
from tpcc_tester.client import RMDBClient
from tpcc_tester.record.record import Recorder
from tpcc_tester.config import get_config

class RMDBDriver(TpccDriver):
    def __init__(self, client: RMDBClient, scale: int, recorder: Recorder = None):
        super().__init__(client, scale, recorder)

    @override
    def load_data(self):
        self.send_file(f"{project_dir}/db/create_index.sql")
        self.send_file(f"{project_dir}/db/load_csvs.sql")
        config = get_config()
        if not config.output_file_on:
            self._client.send_cmd("set output_file off;")