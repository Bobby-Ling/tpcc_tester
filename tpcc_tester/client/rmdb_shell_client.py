# PYTHONPATH=. python test/client_test.py
import pathlib
import sys
import readline

file_path = pathlib.Path(__file__)
file_dir = file_path.parent
sys.path.append(str(file_dir.parent.parent))

from tpcc_tester.client import RMDBClient
from tpcc_tester.common import ServerState
from tpcc_tester.common import setup_logging

class RMDBShellClient():
    def __init__(self):
        self.client = RMDBClient()
        self.client.connect()
        self.logger = setup_logging(self.__class__.__name__)
        readline.parse_and_bind(r'"\\e[A": history-search-backward')
        readline.parse_and_bind(r'"\\e[B": history-search-forward')

    def start_shell_client(self):
        while True:
            command = input("Client> ")

            if not command:
                continue

            readline.add_history(command)

            try:
                result = self.client.send_cmd(command)
                print(result.result_str)
            except Exception as e:
                print(f"Error: {str(e)}")
                continue

if __name__ == "__main__":
    client = RMDBShellClient()
    client.start_shell_client()