import os
import pathlib
import sqlite3
import threading
import matplotlib.pyplot as plt
import numpy as np

file_path = pathlib.Path(__file__)
file_dir = file_path.parent
project_dir = file_dir.parent

NewOrder = 0
Payment = 1
Delivery = 2
OrderStatus = 3
StockLevel = 4
name = {NewOrder: 'New Order', Payment: 'Payment', Delivery: 'Delivery', OrderStatus: 'OrderStatus',
        StockLevel: 'StockLevel'}

class Recorder:
    def __init__(self, db_path: str):
        self.db_path = db_path
        db_dir = pathlib.Path(self.db_path).parent
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
        self.conn = sqlite3.connect(self.db_path)
        self.lock = threading.Lock()
        self.build_db()

    def build_db(self):
        cursor = self.conn.cursor()
        cursor.execute('create table new_order_txn(no integer, time real);')
        cursor.execute('create table test_result(txn integer, avg real, total integer, success integer);')
        cursor.executemany('insert into test_result(txn, avg, total, success) values(?,?,?,?);',
                        [(NewOrder, 0, 0, 0), (Payment, 0, 0, 0), (Delivery, 0, 0, 0), (OrderStatus, 0, 0, 0),
                            (StockLevel, 0, 0, 0)])
        cursor.execute('insert into new_order_txn(no, time) values(?,?);', (0, 0))
        self.conn.commit()

    def put_new_order(self, time: float):
        self.lock.acquire()
        cursor = self.conn.cursor()
        cursor.execute('begin transaction;')
        cursor.execute('select no from new_order_txn order by no desc;')
        no = cursor.fetchone()[0]
        cursor.execute('insert into new_order_txn(no,time) values(?,?);', (no + 1, time))
        self.conn.commit()
        self.lock.release()

    def put_txn(self, txn: int, time: float, success: bool):
        self.lock.acquire()
        cursor = self.conn.cursor()
        cursor.execute('begin transaction;')
        cursor.execute('select avg, total, success from test_result where txn = ?', (txn,))
        avg, total, success_ = cursor.fetchone()
        if not success:
            success = 0
        cursor.execute('update test_result set avg = ?,total = ?, success = ? where txn=?;',
                    (avg + time, total + 1, success_ + success, txn))
        self.conn.commit()
        self.lock.release()


    def analysis(self):
        cursor = self.conn.cursor()
        cursor.execute('select * from test_result;')
        rows = cursor.fetchall()
        result = [{} for i in range(5)]
        for row in rows:
            if row[2] != 0:
                result[row[0]]['avg'] = row[1] / row[2]
            else:
                result[row[0]]['avg'] = 0
            result[row[0]]['total'] = row[2]
            result[row[0]]['success'] = row[3]
            result[row[0]]['name'] = name[row[0]]
        cursor.execute('select * from new_order_txn;')
        new_order_result = cursor.fetchall()
        return result, new_order_result

    def output_result(self) -> int:
        result, new_order_result = self.analysis()

        total_transactions = 0
        total_rollbacks = 0
        statistics_lines = []

        # 计算每个事务的回滚率和总回滚率
        for r in result:
            failure_count = r['total'] - r['success']
            rollback_rate = (failure_count / r['total']) * 100 if r['total'] > 0 else 0

            statistics_lines.append(
                f"{r['name']} - \navg time: {r['avg']}\ntotal: {r['total']}\nsuccess: {r['success']}\nRollback rate: {rollback_rate:.2f}%\n\n")

            print(
                f"{r['name']} - \navg time: {r['avg']}\ntotal: {r['total']}\nsuccess: {r['success']}\nRollback rate: {rollback_rate:.2f}%")

            total_transactions += r['total']
            total_rollbacks += failure_count

        total_rollback_rate = (total_rollbacks / total_transactions) * 100 if total_transactions > 0 else 0
        print(f"Total Rollback Rate: {total_rollback_rate:.2f}%")

        # 写入 statistics_of_five_transactions.txt
        with open(f'{project_dir}/result/statistics_of_five_transactions.txt', 'w') as f:
            f.writelines(statistics_lines)

        # 处理 new order 结果，写入 timecost_and_num_of_NewOrders.txt
        new_order_lines = [f"number: {n[0]}, time cost: {n[1]}\n" for n in new_order_result]
        with open(f'{project_dir}/result/timecost_and_num_of_NewOrders.txt', 'w') as f2:
            f2.writelines(new_order_lines)

        # 画图并保存图像
        times = np.array([e[1] for e in new_order_result])
        numbers = np.array([e[0] for e in new_order_result])

        plt.plot(times, numbers)
        plt.ylabel('Number of New-Orders')
        plt.xlabel('Time unit: second')
        plt.savefig(f'{project_dir}/result/timecost_and_num_of_NewOrders.jpg')
        # plt.show()

        # 删除数据库文件
        if os.path.exists(f'{project_dir}/result/rds.db'):
            os.remove(f'{project_dir}/result/rds.db')

        # 返回 new order 成功数量
        return result[0]['success']
