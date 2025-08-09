from dataclasses import dataclass
from enum import Enum
import pathlib
import sys
from typing import List
import numpy as np
import pandas as pd
from pathlib import Path

file_path = pathlib.Path(__file__)
file_dir = file_path.parent
project_dir = file_dir.parent
base_dir = Path('.')
sys.path.append(str(project_dir.parent))

from tpcc_tester.common import setup_logging

class TpccTransactionType(Enum):
    NewOrder = 0
    Payment = 1
    Delivery = 2
    OrderStatus = 3
    StockLevel = 4

@dataclass
class TxnRecord:
    type: TpccTransactionType
    start_time: int
    end_time: int
    success: bool

class ProcessTxnRecorder:
    def __init__(self, name: str = "all"):
        self.logger = setup_logging(f"{__name__}")
        self.name = name
        self.transaction_records: list[TxnRecord] = []

    def put_txn(self, txn: TpccTransactionType, start_time: int, end_time: int, success: bool):
        self.logger.info(f"put_txn: txn: {txn}, start_time: {start_time}, end_time: {end_time}, success: {success}")
        self.transaction_records.append(TxnRecord(txn, start_time, end_time, success))

    def to_df(self):
        data = []
        for record in self.transaction_records:
            data.append({
                'type': record.type.value,
                'type_name': record.type.name,
                'start_time': record.start_time,
                'end_time': record.end_time,
                'time': record.end_time - record.start_time,
                'success': record.success
            })

        df = pd.DataFrame(data)
        return df

    def save(self):
        df = self.to_df()
        csv_file = f'{base_dir.absolute()}/result/records_{self.name}.csv'
        df.to_csv(csv_file, index=False, sep ='\t')
        print(f"save records to {csv_file}")

    @staticmethod
    def merge_records(records: List['ProcessTxnRecorder']):
        merged_recorder = ProcessTxnRecorder()
        for recorder in records:
            merged_recorder.transaction_records.extend(recorder.transaction_records)
        print(f"merge records from {len(records)} process(es), total {len(merged_recorder.transaction_records)} transactions")
        return merged_recorder

    def analysis(self):
        df = self.to_df()
        # 按事务类型分组并计算统计信息
        result_df = df.groupby(['type_name']).agg({
            'time': 'mean',
            'success': ['sum', 'count']
        })

        result_df.columns = ['avg_time(ns)', 'success', 'total']
        result_df = result_df.reset_index()

        # 计算rollback_rate
        result_df['fail'] = result_df['total'] - result_df['success']
        result_df['rollback_rate(%)'] = (result_df['fail'] / result_df['total']) * 100

        # time.time_ns() -> ms
        result_df['avg_time(ms)'] = result_df['avg_time(ns)'] / 1_000_000.0
        result_df.drop(columns=['avg_time(ns)'], inplace=True)

        statistics_file = f'{base_dir.absolute()}/result/statistics.csv'
        result_df.to_csv(statistics_file, index=False, sep ='\t')
        print(f"save statistics to {statistics_file}")

        return df, result_df

    def output_result(self) -> int:
        df, result_df = self.analysis()

        total_transactions = result_df['total'].sum()
        total_rollbacks = result_df['fail'].sum()

        total_rollback_rate = (total_rollbacks / total_transactions) * 100 if total_transactions > 0 else 0
        print(result_df)
        print(f"Total Rollback Rate: {total_rollback_rate:.2f}%")

        # 返回NewOrdersuccess量
        new_order_success = result_df[result_df['type_name'] == 'NewOrder']['success'].sum() if not result_df.empty else 0
        return int(new_order_success)
