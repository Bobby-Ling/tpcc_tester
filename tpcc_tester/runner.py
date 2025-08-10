import multiprocessing
import time
from typing import List
from concurrent.futures import ProcessPoolExecutor, Future
from multiprocessing.synchronize import Lock as LockBase
import pathlib
import sys

file_path = pathlib.Path(__file__)
file_dir = file_path.parent
project_dir = file_dir
base_dir = '.'
sys.path.append(str(project_dir.parent))

from tpcc_tester.client import *
from tpcc_tester.record.process_record import ProcessTxnRecorder
from tpcc_tester.driver.tpcc_driver import TpccDriver
from tpcc_tester.common import setup_logging
from tpcc_tester.config import get_config

config = get_config()

# TestRunner只需要一个就行
class TestRunner:
    def __init__(self, client_type: ClientType):
        self.client_type = client_type
        self.logger = setup_logging(f"{__name__}")

    def clean(self, drop_db: bool = False):
        # shutil.rmtree(f'{base_dir}/result')
        # os.mkdir(f'{base_dir}/result')
        driver = TpccDriver.from_type(self.client_type, scale=1, recorder=None)
        if drop_db:
            try:
                driver.drop()
            except:
                pass

    def prepare(self):
        # Driver是每次任务一个
        driver = TpccDriver.from_type(self.client_type, scale=1, recorder=None)
        driver.build()  # 创建9个tables
        # driver.send_sql_from_dir(f'{project_dir}/../../tpcc-generator/tpcc_sql/')
        # driver.send_file(f'{project_dir}/../../tpcc-generator/demo.sql')

        driver.load_data()

        if config.validate:
            driver.count_star()
            driver.consistency_check()  # 一致性校验
        # driver.build()  # 创建9个tables
        # driver.create_index() # 建立除history表外其余表的索引
        # driver.load()  # 加载csv数据到9张表
        driver.delay_close()

    def test(self, tid, txns=150, txn_prob=None, seed: int=42, global_lock: LockBase = None):
        self.logger.info(f'+ Test_{tid} Begin(txns: {txns}, txn_prob: {txn_prob}, seed: {seed})')
        # Driver每个线程一个
        # random seed 不会从父进程复制
        # https://152334h.github.io/blog/multiprocessing-and-random/
        import random
        random.seed(seed + tid)
        if config.analyze:
            recorder = ProcessTxnRecorder(f'{tid}')
        else:
            recorder = None

        driver = TpccDriver.from_type(self.client_type, scale=config.CNT_W, recorder=recorder, global_lock=global_lock)
        try:
            driver.run_test(txns, txn_prob)
        except KeyboardInterrupt:
            self.logger.info(f'Test_{tid} Canceled')
        self.logger.info(f'- Test_{tid} Finished')
        driver.delay_close()
        return recorder

# useage: python runner.py --prepare --thread 8 --rw 150 --ro 150 --analyze
def main():
    print(f"config: {config}")

    runner = TestRunner(config.client_type)

    if config.global_lock:
        m = multiprocessing.Manager()
        global_lock = m.Lock()
    else:
        global_lock = None

    if config.clean:
        print("clean all tables!!!")
        runner.clean(drop_db=True)
        return

    # 有perpare说明要drop表
    runner.clean(drop_db=config.prepare)
    if config.prepare:
        lt1 = time.time()
        runner.prepare()
        runner.logger.info(f'load time: {time.time() - lt1}')

    t1 = 0
    t2 = 0
    t3 = 0
    futures: List[Future] = []
    if config.thread_num:
        t1 = time.time()
        if config.rw:
            with ProcessPoolExecutor(max_workers=config.thread_num) as executor:
                for i in range(config.thread_num):
                    future = executor.submit(runner.test, i + 1, config.rw // config.thread_num, [10 / 23, 10 / 23, 1 / 23, 1 / 23, 1 / 23], config.seed, global_lock)
                    futures.append(future)

        t2 = time.time()
        if config.ro:
            with ProcessPoolExecutor(max_workers=config.thread_num) as executor:
                for i in range(config.thread_num):
                    future = executor.submit(runner.test, 1, config.ro // config.thread_num, [0, 0, 0, 0.5, 0.5], config.seed, global_lock)
                    futures.append(future)

        t3 = time.time()

    if config.analyze:
        records: List[ProcessTxnRecorder] = []

        for future in futures:
            records.append(future.result())

        all_records = ProcessTxnRecorder.merge_records(records)
        all_records.save()
        new_order_success = all_records.output_result()

    if config.validate:
        driver = TpccDriver.from_type(config.client_type, scale=config.warehouse, recorder=None)
        driver.consistency_check()

        if config.analyze:
            driver.consistency_check2(new_order_success)

    if config.analyze:
        print(f'total time of rw txns: {t2 - t1}')
        print(f'total time of ro txns: {t3 - t2}')
        print(f'total time: {t3 - t1}')
        print(f'tpmC: {new_order_success / ((t3 - t1) / 60)}')


if __name__ == '__main__':
    main()
