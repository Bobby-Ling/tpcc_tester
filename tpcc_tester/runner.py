import argparse
from enum import Enum
import os
import shutil
import time
from typing import List
from multiprocessing import Process
import pathlib
import sys

file_path = pathlib.Path(__file__)
file_dir = file_path.parent
project_dir = file_dir
sys.path.append(str(project_dir.parent))

from tpcc_tester.client import *
from tpcc_tester.record.record import Recorder, get_recorder_instance
from tpcc_tester.driver.tpcc_driver import CNT_W, TpccDriver
from tpcc_tester.common import setup_logging

# TestRunner只需要一个就行
class TestRunner:
    def __init__(self, recorder: Recorder, client_type: ClientType):
        # Recorder是全局唯一的
        self.recorder = recorder
        self.client_type = client_type
        self.logger = setup_logging(f"{__name__}")

    def clean(self, drop_db: bool = False):
        # shutil.rmtree(f'{project_dir}/result')
        # os.mkdir(f'{project_dir}/result')
        driver = TpccDriver.from_type(self.client_type, scale=1, recorder=None)
        if drop_db:
            try:
                driver.drop()
            except:
                pass

    def prepare(self):
        # Driver是每次任务一个
        driver = TpccDriver.from_type(self.client_type, scale=1, recorder=self.recorder)
        driver.build()  # 创建9个tables
        # driver.send_sql_from_dir(f'{project_dir}/../../tpcc-generator/tpcc_sql/')
        # driver.send_file(f'{project_dir}/../../tpcc-generator/demo.sql')

        driver.load_data()

        driver.count_star()
        driver.consistency_check()  # 一致性校验
        # driver.build()  # 创建9个tables
        # driver.create_index() # 建立除history表外其余表的索引
        # driver.load()  # 加载csv数据到9张表
        driver.delay_close()

    def test(self, tid, txns=150, txn_prob=None):
        self.logger.info(f'+ Test_{tid} Begin(txns: {txns}, txn_prob: {txn_prob})')
        # Driver每个线程一个
        driver = TpccDriver.from_type(self.client_type, scale=CNT_W, recorder=self.recorder)
        driver.run_test(txns, txn_prob)
        self.logger.info(f'- Test_{tid} Finished')
        driver.delay_close()

# useage: python runner.py --prepare --thread 8 --rw 150 --ro 150 --analyze
def main():
    parser = argparse.ArgumentParser(description='Python Script with Thread Number Argument')
    parser.add_argument('--prepare', action='store_true', help='Enable prepare mode')
    parser.add_argument('--analyze', action='store_true', help='Enable analyze mode')
    parser.add_argument('--clean', action='store_true', help='Clean database(execlude with other options)')
    parser.add_argument('--rw', type=int, help='Read write transaction phase time')
    parser.add_argument('--ro', type=int, help='Read only transaction phase time')
    parser.add_argument('--thread', type=int, help='Thread number')
    parser.add_argument('--client', type=str, default='rmdb', choices=['rmdb', 'mysql', 'slt', 'sql'], help='Client type')

    args = parser.parse_args()
    prepare: bool = args.prepare
    analyze: bool = args.analyze
    clean: bool = args.clean
    rw: int = args.rw
    ro: int = args.ro
    thread_num: int = args.thread
    client_type = ClientType(args.client)

    print(f"prepare: {prepare}, analyze: {analyze}, clean: {clean}, rw: {rw}, ro: {ro}, thread: {thread_num}, client: {client_type}")

    recorder = get_recorder_instance()
    runner = TestRunner(recorder, client_type)

    if clean:
        print("clean all tables!!!")
        runner.clean(drop_db=True)
        return

    # 有perpare说明要drop表
    runner.clean(drop_db=prepare)
    if prepare:
        lt1 = time.time()
        runner.prepare()
        runner.logger.info(f'load time: {time.time() - lt1}')

    t1 = 0
    t2 = 0
    t3 = 0
    if thread_num:
        t1 = time.time()
        process_list: List[Process] = []
        if rw:
            for i in range(thread_num):
                process_list.append(
                    Process(target=runner.test, args=(i + 1, rw, [10 / 23, 10 / 23, 1 / 23, 1 / 23, 1 / 23])))
                process_list[i].start()

            for i in range(thread_num):
                process_list[i].join()
        t2 = time.time()
        process_list = []
        if ro:
            for i in range(thread_num):
                process_list.append(Process(target=runner.test, args=(i + 1, ro, [0, 0, 0, 0.5, 0.5])))
                process_list[i].start()

            for i in range(thread_num):
                process_list[i].join()
        t3 = time.time()

    driver = TpccDriver.from_type(client_type, scale=CNT_W, recorder=None)
    driver.consistency_check()

    new_order_success = recorder.output_result()
    driver.consistency_check2(new_order_success)

    if analyze:
        print(f'total time of rw txns: {t2 - t1}')
        print(f'total time of ro txns: {t3 - t2}')
        print(f'total time: {t3 - t1}')
        print(f'tpmC: {new_order_success / ((t3 - t1) / 60)}')


if __name__ == '__main__':
    main()
