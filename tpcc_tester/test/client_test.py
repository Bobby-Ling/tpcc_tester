# PYTHONPATH=. python test/client_test.py
import pathlib
import sys
from typing import List

file_path = pathlib.Path(__file__)
file_dir = file_path.parent
sys.path.append(str(file_dir.parent.parent))

import unittest
from tpcc_tester.client import DBClient, RMDBClient, MySQLClient, SLTClient, SQLClient
from tpcc_tester.common import ServerState
from tpcc_tester.common import setup_logging

class ClientTestCase(unittest.TestCase):
    def setUp(self):
        self.clients: List[DBClient] = [
            RMDBClient(),
            MySQLClient(db="tpcc_test"),
            # SLTClient(slt_file="outputs/test.slt"),
            # SQLClient(sql_file="outputs/test.sql"),
        ]

        self.logger = setup_logging(file_path.name)

    def tearDown(self):
        for client in self.clients:
            client.send_cmd("DROP TABLE test;")
            client.close()

    def test_all(self):
        self.connect()
        self.create_table()
        self.insert()
        self.update()
        self.delete()
        self.transaction()

    def connect(self):
        for client in self.clients:
            self.assertEqual(client.connect(), ServerState.OK)

    def create_table(self):
        for client in self.clients:
            result = client.send_ddl("CREATE TABLE test (id INT, name CHAR(255));")
            self.assertEqual(result.state, ServerState.OK)
            result = client.send_dql("SHOW TABLES;")
            self.assertEqual(result.state, ServerState.OK)
            self.assertEqual(result.data, [["test"]])
            self.logger.info("\n" + result.result_str)

    def insert(self):
        for client in self.clients:
            result = client.send_dml("INSERT INTO test VALUES (1, 'test');")
            self.assertEqual(result.state, ServerState.OK)
            result = client.send_dql("SELECT * FROM test;")
            self.assertEqual(result.state, ServerState.OK)
            self.assertEqual(result.data, [['1', 'test']])
            self.logger.info("\n" + result.result_str)

    def update(self):
        for client in self.clients:
            result = client.send_dml("UPDATE test SET name = 'test2' WHERE id = 1;")
            self.assertEqual(result.state, ServerState.OK)
            result = client.send_dql("SELECT * FROM test;")
            self.assertEqual(result.state, ServerState.OK)
            self.assertEqual(result.data, [['1', 'test2']])
            self.logger.info("\n" + result.result_str)

    def delete(self):
        for client in self.clients:
            result = client.send_dml("DELETE FROM test WHERE id = 1;")
            self.assertEqual(result.state, ServerState.OK)
            result = client.send_dql("SELECT * FROM test;")
            self.assertEqual(result.state, ServerState.OK)
            self.assertEqual(result.data, [])
            self.logger.info("\n" + result.result_str)

    def transaction(self):
        for client in self.clients:
            result = client.begin()
            self.assertEqual(result.state, ServerState.OK)
            result = client.send_dml("INSERT INTO test VALUES (2, 'test2');")
            self.assertEqual(result.state, ServerState.OK)
            result = client.commit()
            self.assertEqual(result.state, ServerState.OK)
            result = client.send_dql("SELECT * FROM test;")
            self.assertEqual(result.state, ServerState.OK)
            self.assertEqual(result.data, [['2', 'test2']])
            self.logger.info("\n" + result.result_str)

    def rollback(self):
        for client in self.clients:
            result = client.begin()
            self.assertEqual(result.state, ServerState.OK)
            result = client.send_dml("INSERT INTO test VALUES (3, 'test3');")
            self.assertEqual(result.state, ServerState.OK)
            result = client.abort()
            self.assertEqual(result.state, ServerState.OK)
            result = client.send_dql("SELECT * FROM test;")
            self.assertEqual(result.state, ServerState.OK)
            self.assertEqual(result.data, [['2', 'test2']])
            self.logger.info("\n" + result.result_str)

if __name__ == "__main__":
    # suite = unittest.TestSuite()
    # suite.addTest(ClientTestCase('test_all'))
    # runner = unittest.TextTestRunner()
    # runner.run(suite)
    unittest.main()