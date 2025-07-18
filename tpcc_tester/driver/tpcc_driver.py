import re
import time
import pathlib
from pathlib import Path
from tqdm import tqdm

from tpcc_tester.client.base import *
from tpcc_tester.common import setup_logging
from tpcc_tester.db.table_layouts import *
from tpcc_tester.client import *
from tpcc_tester.util import *
from tpcc_tester.record.record import *

file_path = pathlib.Path(__file__)
file_dir = file_path.parent
project_dir = file_dir.parent


CNT_W = 2
CNT_ITEM = 100000
CNT_STOCK = CNT_W * 100000
CNT_DISTRICT = CNT_W * 10
CNT_CUSTOMER = CNT_W * 10 * 3000
CNT_HISTORY = CNT_W * 10 * 3000
CNT_ORDERS = CNT_W * 10 * 3000
CNT_NEW_ORDERS = CNT_W * 10 * 900
# CNT_ORDER_LINE = CNT_ORDERS * 10
CNT_ORDER_LINE = 601255

"""
wc -l tpcc_sql/*.sql
    60000 tpcc_sql/customer.sql
       20 tpcc_sql/district.sql
    60000 tpcc_sql/history.sql
   100000 tpcc_sql/item.sql
    18000 tpcc_sql/new_orders.sql
   601255 tpcc_sql/order_line.sql
    60000 tpcc_sql/orders.sql
   200000 tpcc_sql/stock.sql
        2 tpcc_sql/warehouse.sql
  1099277 总计

wc -l tpcc_csv/*.csv
    60001 tpcc_csv/customer.csv
       21 tpcc_csv/district.csv
    60001 tpcc_csv/history.csv
   100001 tpcc_csv/item.csv
    18001 tpcc_csv/new_orders.csv
   601256 tpcc_csv/order_line.csv
    60001 tpcc_csv/orders.csv
   200001 tpcc_csv/stock.csv
        3 tpcc_csv/warehouse.csv
  1099286 总计
"""

# 定义每个表的参数
tables_info = [
    (WAREHOUSE, 'count_warehouse', CNT_W, 'count_warehouse'),
    (DISTRICT, 'count_district', CNT_DISTRICT, 'count_district'),
    (CUSTOMER, 'count_customer', CNT_CUSTOMER, 'count_customer'),
    (HISTORY, 'count_history', CNT_HISTORY, 'count_history'),
    (NEW_ORDERS, 'count_new_orders', CNT_NEW_ORDERS, 'count_new_orders'),
    (ORDERS, 'count_orders', CNT_ORDERS, 'count_orders'),
    (ORDER_LINE, 'count_order_line', CNT_ORDER_LINE, 'count_order_line'),
    (ITEM, 'count_item', CNT_ITEM, 'count_item'),
    (STOCK, 'count_stock', CNT_STOCK, 'count_stock')
]

W_ID_MAX = 51
D_ID_MAX = 11


class TpccDriver:
    def __init__(self, client: DBClient, scale: int, recorder: Recorder = None):
        self._scale = scale
        self._client = client
        self._recorder = recorder

        self.logger = setup_logging(f"{self.__class__.__name__}")
        self._flag = True
        # self._delivery_q = Queue()
        # self._delivery_t = Thread(target=self.process_delivery, args=(self._delivery_q,))
        # self._delivery_t.start()
        # self._delivery_stop = False
        assert self._client.connect() == ServerState.OK

    def load_data(self) -> bool:
        pass

    def run_test(self, txns, txn_prob=None):
        # self.logger.info(duration)
        # self.logger.info('Test')
        t1 = 0
        t2 = 0

        w_id = 0
        d_id = 0
        c_id = 0
        ol_i_id = 0
        ol_supply_w_id = 0
        ol_quantity = 0
        o_carrier_id = 0
        c_w_id = 0
        c_d_id = 0
        h_amount = 0
        query_cus = 0
        threshold = 0

        if txn_prob is None:
            txn_prob = [10 / 23, 10 / 23, 1 / 23, 1 / 23, 1 / 23]

        t_start = time.time()

        for i in range(txns):
            txn = get_choice(txn_prob)
            ret = ServerState.ABORT

            # 预生成操作
            if txn == 0:  # NewOrder
                w_id = get_w_id()
                d_id = get_d_id()  # 获得地区id，1～10的随机数
                c_id = get_c_id()  # 获得客户id，1～3000的随机数
                ol_i_id = get_ol_i_id()  # 获得新订单中的商品id列表
                ol_supply_w_id = get_ol_supply_w_id(w_id, self._scale, len(ol_i_id))  # 为新订单中每个商品选择一个供应仓库，当前设定就一个供应仓库
                ol_quantity = get_ol_quantity(len(ol_i_id))  # 为新订单中每个商品设置购买数量

            elif txn == 1:  # Payment
                w_id = get_w_id()
                d_id = get_d_id()  # 获得地区id，1～10的随机数
                query_cus = query_cus_by(True)
                h_amount = get_h_amount()
                c_w_id, c_d_id = get_c_w_id_d_id(w_id, d_id, self._scale)  # 获得客户所属的仓库id和地区id

            elif txn == 2:  # Delivery
                w_id = get_w_id()
                o_carrier_id = get_o_carrier_id()

            elif txn == 3:  # OrderStatus
                w_id = get_w_id()
                d_id = get_d_id()  # 获得地区id，1～10的随机数
                query_cus = query_cus_by()

            elif txn == 4:  # StockLevel
                w_id = get_w_id()
                d_id = get_d_id()  # 获得地区id，1～10的随机数
                threshold = random.randrange(10, 21)

            while ret == ServerState.ABORT:
                if txn == 0:  # NewOrder
                    t1 = time.time()
                    ret = self.do_new_order(w_id, d_id, c_id, ol_i_id, ol_supply_w_id, ol_quantity)
                    t2 = time.time()
                    if self._recorder:
                        self._recorder.put_new_order(t2 - t_start)

                elif txn == 1:  # Payment
                    t1 = time.time()
                    ret = self.do_payment(w_id, d_id, c_w_id, c_d_id, query_cus, h_amount)
                    t2 = time.time()

                elif txn == 2:  # Delivery
                    t1 = time.time()
                    ret = self.do_delivery(w_id, o_carrier_id)
                    t2 = time.time()

                elif txn == 3:  # OrderStatus
                    t1 = time.time()
                    ret = self.do_order_status(w_id, d_id, query_cus)
                    t2 = time.time()

                elif txn == 4:  # StockLevel
                    t1 = time.time()
                    ret = self.do_stock_level(w_id, d_id, threshold)
                    t2 = time.time()

                # if ret != SQLState.ABORT:
                #     put_txn(lock, txn, t2 - t1, True)

                if ret == ServerState.ABORT:
                    if self._recorder:
                        self._recorder.put_txn(txn, t2 - t1, False)
                else:
                    if self._recorder:
                        self._recorder.put_txn(txn, t2 - t1, True)

        # for i in range(txns):
        #     txn = get_choice(txn_prob)
        #     ret = SQLState.ABORT
        #     while ret == SQLState.ABORT:
        #         if txn == 0:  # NewOrder
        #             w_id = get_w_id()
        #             d_id = get_d_id()  # 获得地区id，1～10的随机数
        #             c_id = get_c_id()  # 获得客户id，1～3000的随机数
        #             ol_i_id = get_ol_i_id()  # 获得新订单中的商品id列表
        #             ol_supply_w_id = get_ol_supply_w_id(w_id, driver._scale, len(ol_i_id))  # 为新订单中每个商品选择一个供应仓库，当前设定就一个供应仓库
        #             ol_quantity = get_ol_quantity(len(ol_i_id))  # 为新订单中每个商品设置购买数量
        #
        #             t1 = time.time()
        #             ret = driver.do_new_order(w_id, d_id, c_id, ol_i_id, ol_supply_w_id, ol_quantity)
        #             t2 = time.time()
        #
        #             put_new_order(lock, t2 - t_start)
        #
        #         elif txn == 1:  # Payment
        #             w_id = get_w_id()
        #             d_id = get_d_id()  # 获得地区id，1～10的随机数
        #             c_w_id, c_d_id = get_c_w_id_d_id(w_id, d_id, driver._scale)  # 获得客户所属的仓库id和地区id
        #
        #             t1 = time.time()
        #             ret = driver.do_payment(w_id, d_id, c_w_id, c_d_id, query_cus_by(), random.random() * (5000 - 1) + 1)
        #             t2 = time.time()
        #
        #         elif txn == 2:  # Delivery
        #             w_id = get_w_id()
        #             t1 = time.time()
        #             ret = driver.do_delivery(w_id, get_o_carrier_id())
        #             t2 = time.time()
        #
        #         elif txn == 3:  # OrderStatus
        #             w_id = get_w_id()
        #             t1 = time.time()
        #             ret = driver.do_order_status(w_id, get_d_id(), query_cus_by())
        #             t2 = time.time()
        #
        #         elif txn == 4:  # StockLevel
        #             w_id = get_w_id()
        #             t1 = time.time()
        #             ret = driver.do_stock_level(w_id, get_d_id(), random.randrange(10, 21))
        #             t2 = time.time()
        #
        #         if ret == SQLState.ABORT:
        #             put_txn(lock, txn, t2 - t1, False)
        #         else:
        #             put_txn(lock, txn, t2 - t1, True)

    def delay_close(self):
        self._flag = False
        # while not self._delivery_stop:
        #     continue
        self._client.close()

    # def close(self):
    #     self._client.close()

    def send_file(self, file_path: str):
        sql = [line.strip() for line in open(file_path, "r").read().split(';') if line and line.strip() != '']
        for line in tqdm(sql, desc=f"Sending {Path(file_path).name}"):
            self._client.send_cmd(line + ';')

    def build(self):
        self.logger.info("Build table schema...")
        self.send_file(f"{project_dir}/db/create_tables.sql")

    def drop(self):
        self.logger.info("Drop table schema...")
        self.send_file(f"{project_dir}/db/drop_table.sql")

    def load(self):
        self.logger.info("Load table data...")
        self.send_file(f"{project_dir}/db/load_csvs.sql")
        self.logger.info('Database has been initialized.')

    def create_index(self):
        self.logger.info("Create index...")
        self.send_file(f"{project_dir}/db/create_index.sql")

    def load_csv(self):
        self.logger.info("Loading data...")
        self.send_file(f"{project_dir}/db/load_csvs.sql")

    def send_sql_from_dir(self, sql_dir: str):
        sql_files = [f"{file}" for file in os.listdir(sql_dir) if file.endswith('.sql')]
        for sql_file in sql_files:
            self.send_file(f"{sql_dir}/{sql_file}")

    def count_and_check(self, table, count_as, expected_count, count_type):
        """
        A helper function to count and check the result.

        :param client: The database client.
        :param table: The table to perform the count on.
        :param count_as: The alias for the count column.
        :param expected_count: The expected count value.
        :param count_type: Descriptive type of count for error messages.
        :return: None
        """
        count_result = 0
        res = self._client.select(table=table, col=(COUNT(alias=count_as),))
        try:
            count_result = int(res.data[0][0])
        except IndexError:
            self.logger.info(f'error, {count_type}: {count_result}, expecting: {expected_count}')
            return
        if count_result != expected_count:
            self.logger.info(f'failed, {count_type}: {count_result}, expecting: {expected_count}')

    def count_star(self):
        self.logger.info("Count star...")
        # 遍历每个表的信息并进行检查
        for table, count_as, expected_count, count_type in tables_info:
            self.count_and_check(table, count_as, expected_count, count_type)

    def consistency_check(self):
        self.logger.info("consistency checking...")

        w_id = 0
        d_id = 0

        try:
            for w_id in range(1, W_ID_MAX):
                for d_id in range(1, D_ID_MAX):
                    res = self._client.select(
                                 table=DISTRICT,
                                 col=(D_NEXT_O_ID,),  # 加逗号，否则会被认为是字符串，而不是元组
                                 where=[(D_W_ID, EQ, w_id),
                                        (D_ID, EQ, d_id)])
                    if res.state == ServerState.ABORT:
                        raise Exception(f"error: {w_id}, {d_id}")

                    d_next_o_id = int(res.data[0][0])

                    res = self._client.select(
                                 table=ORDERS,
                                 col=(MAX(O_ID),),
                                 where=[(O_W_ID, EQ, w_id),
                                        (O_D_ID, EQ, d_id)])

                    if res is None or len(res.data[0]) == 0:
                        raise Exception(f"error: {w_id}, {d_id}")

                    max_o_id = int(res.data[0][0])

                    res = self._client.select(
                                 table=NEW_ORDERS,
                                 col=(MAX(NO_O_ID),),
                                 where=[(NO_W_ID, EQ, w_id),
                                        (NO_D_ID, EQ, d_id)])

                    if res is None:
                        raise Exception(f"error: {w_id}, {d_id}")

                    max_no_o_id = int(res.data[0][0])

                    if d_next_o_id - 1 != max_o_id or d_next_o_id - 1 != max_no_o_id:
                        self.logger.info(
                            f"d_next_o_id={d_next_o_id}, max(o_id)={max_o_id}, max(no_o_id)={max_no_o_id} when d_id={d_id} and w_id={w_id}")

            self.logger.info("consistency check for district, orders and new_orders pass!")

            for w_id in range(1, W_ID_MAX):
                for d_id in range(1, D_ID_MAX):
                    res = self._client.select(
                                 table=NEW_ORDERS,
                                 col=(COUNT(NO_O_ID),),
                                 where=[(NO_W_ID, EQ, w_id),
                                        (NO_D_ID, EQ, d_id)])

                    num_no_o_id = int(res.data[0][0])

                    res = self._client.select(
                                 table=NEW_ORDERS,
                                 col=(MAX(NO_O_ID),),
                                 where=[(NO_W_ID, EQ, w_id),
                                        (NO_D_ID, EQ, d_id)])

                    max_no_o_id = int(res.data[0][0])

                    res = self._client.select(
                                 table=NEW_ORDERS,
                                 col=(MIN(NO_O_ID),),
                                 where=[(NO_W_ID, EQ, w_id),
                                        (NO_D_ID, EQ, d_id)])

                    min_no_o_id = int(res.data[0][0])

                    if num_no_o_id != max_no_o_id - min_no_o_id + 1:
                        self.logger.info(
                            f"count(no_o_id)={num_no_o_id}, max(no_o_id)={max_no_o_id}, min(no_o_id)={min_no_o_id} when d_id={d_id} and w_id={w_id}")

            self.logger.info("consistency check for new_orders pass!")

            for w_id in range(1, W_ID_MAX):
                for d_id in range(1, D_ID_MAX):
                    res = self._client.select(
                                 table=ORDERS,
                                 col=(SUM(O_OL_CNT),),
                                 where=[(O_W_ID, EQ, w_id),
                                        (O_D_ID, EQ, d_id)])

                    sum_o_ol_cnt = int(res.data[0][0])

                    res = self._client.select(
                                 table=ORDER_LINE,
                                 col=(COUNT(OL_O_ID),),
                                 where=[(OL_W_ID, EQ, w_id),
                                        (OL_D_ID, EQ, d_id)])

                    num_ol_o_id = int(res.data[0][0])

                    if sum_o_ol_cnt != num_ol_o_id:
                        self.logger.info(
                            f"sum(o_ol_cnt)={sum_o_ol_cnt}, count(ol_o_id)={num_ol_o_id} when d_id={d_id} and w_id={w_id}")

            self.logger.info("consistency check for orders and order_line pass!")

        except Exception as e:
            self.logger.exception(f"Exception occurred in w_id: {w_id}, d_id: {d_id}, error: {e}")
            raise e

    def consistency_check2(self, cnt_new_orders):
        self.logger.info("consistency checking 2...")
        try:
            res = self._client.select(
                         table=ORDERS,
                         col=(COUNT(alias='count_orders'),),
                         )
            cnt_orders = int(res.data[0][0])
            if cnt_orders == CNT_ORDERS + cnt_new_orders:
                self.logger.info("all pass!")
                return True
            self.logger.info(
                f"count(*)={cnt_orders}, count(new_orders)={cnt_new_orders} when origin orders={CNT_ORDERS}")
        except Exception as e:
            self.logger.info(e)
        self.logger.info("consistency checking 2 error!")

    def do_new_order(self, w_id, d_id, c_id, ol_i_id, ol_supply_w_id, ol_quantity):
        res = []
        ol_cnt = len(ol_i_id)
        ol_amount = 0
        total_amount = 0
        brand_generic = ''
        s_data = ''

        # transcation
        if self._client.send_cmd("BEGIN;").state == ServerState.ABORT:
            return ServerState.ABORT
        # self.logger.info('+ New Order')
        # phase 1
        # 检索仓库（warehouse）税率、区域（district）税率和下一个可用订单号。
        try:
            res = self._client.select(
                         table=DISTRICT,
                         col=(D_TAX, D_NEXT_O_ID),
                         where=[(D_ID, EQ, d_id),
                                (D_W_ID, EQ, w_id)])

            # 每一个都加上判断
            if res.state == ServerState.ABORT:
                return ServerState.ABORT

            d_tax, d_next_o_id = res.data[0]
            d_tax = float(d_tax)
            d_next_o_id = int(d_next_o_id)
        except Exception as e:
            self.logger.info(f"Exception occurred in w_id: {w_id}, d_id: {d_id}, res: {res}")
            self.logger.info(e)
            # exit(1)
            d_tax = 0
            d_next_o_id = 0

        if self._client.update(
                  table=DISTRICT,
                  row=(D_NEXT_O_ID, d_next_o_id + 1),
                  where=[(D_ID, EQ, d_id), (D_W_ID, EQ, w_id)]).state == ServerState.ABORT:
            return ServerState.ABORT

        try:
            res = self._client.select(
                         col=(C_DISCOUNT, C_LAST, C_CREDIT, W_TAX),
                         table=[CUSTOMER, WAREHOUSE],
                         where=[(W_ID, EQ, w_id), (C_W_ID, EQ, W_ID), (C_D_ID, EQ, d_id), (C_ID, EQ, c_id)]
                         )
            if res.state == ServerState.ABORT:
                return ServerState.ABORT
            c_discount, c_last_, c_credit, w_tax = res.data[0]
            c_discount = float(c_discount)
            w_tax = float(w_tax)
        except Exception as e:
            self.logger.info('error', CUSTOMER, WAREHOUSE)
            # exit(1)
            c_discount = 0
            w_tax = 0

        # phase 2
        # 插入订单（order）、新订单（new-order）和新订单行（order-line）。
        order_time = "'" + current_time() + "'"
        if self._client.insert(
                  table=ORDERS,
                  rows=(d_next_o_id, d_id, w_id, c_id, order_time, 0, ol_cnt,
                        int(len(set(ol_supply_w_id)) == 1))).state == ServerState.ABORT:
            return ServerState.ABORT

        if self._client.insert(
                  table=NEW_ORDERS,
                  rows=(d_next_o_id, d_id, w_id)).state == ServerState.ABORT:
            return ServerState.ABORT

        # phase 3
        for i in range(ol_cnt):
            try:
                res = self._client.select(
                             table=ITEM,
                             col=(I_PRICE, I_NAME, I_DATA),
                             where=(I_ID, EQ, ol_i_id[i]))
                if res.state == ServerState.ABORT:
                    return ServerState.ABORT
                i_price, i_name, i_data = res.data[0]
                i_price = float(i_price)
            except Exception as e:
                self.logger.info('error', ITEM)
                # exit(1)
                i_price = 1
                i_data = 'null'

            try:
                res = self._client.select(
                             table=STOCK,
                             col=(
                                 S_QUANTITY, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06,
                                 S_DIST_07,
                                 S_DIST_08, S_DIST_09, S_DIST_10, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA),
                             where=[(S_I_ID, EQ, ol_i_id[i]),
                                    (S_W_ID, EQ, ol_supply_w_id[i])])
                if res.state == ServerState.ABORT:
                    return ServerState.ABORT
                s_quantity, *s_dist, s_ytd, s_order_cnt, s_remote_cnt, s_data = res.data[0]
                s_quantity = float(s_quantity)
                s_ytd = float(s_ytd)
                s_order_cnt = float(s_order_cnt)
                s_remote_cnt = float(s_remote_cnt)
            except Exception as e:
                self.logger.info('error', STOCK)
                # exit(1)
                s_quantity = 0
                s_ytd = 0
                s_order_cnt = 0
                s_remote_cnt = 0
                s_dist = []

            if s_quantity - ol_quantity[i] >= 10:
                s_quantity -= ol_quantity[i]
            else:
                s_quantity = s_quantity - ol_quantity[i] + 91

            s_ytd += ol_quantity[i]
            s_order_cnt += 1

            if ol_supply_w_id[i] != w_id:
                s_remote_cnt += 1

            if self._client.update(
                      table=STOCK,
                      row=[(S_QUANTITY, s_quantity),
                           (S_YTD, s_ytd),
                           (S_ORDER_CNT, s_order_cnt),
                           (S_REMOTE_CNT, s_remote_cnt)],
                      where=[(S_I_ID, EQ, ol_i_id[i]),
                             (S_W_ID, EQ, ol_supply_w_id[i])]).state == ServerState.ABORT:
                return ServerState.ABORT
            ol_amount = ol_quantity[i] * i_price
            brand_generic = 'B' if re.search('ORIGINAL', i_data) and re.search('ORIGINAL', s_data) else 'G'

            try:
                if self._client.insert(
                          table=ORDER_LINE,
                          rows=(d_next_o_id, d_id, w_id, i, ol_i_id[i], ol_supply_w_id[i], order_time, ol_quantity[i],
                                ol_amount, "'" + s_dist[d_id - 1] + "'")).state == ServerState.ABORT:
                    return ServerState.ABORT

            except Exception as e:
                self.logger.info('error', ORDER_LINE)
                pass

            total_amount += ol_amount

        total_amount *= (1 - c_discount) * (1 + w_tax + d_tax)

        if self._client.send_cmd("COMMIT;").state == ServerState.ABORT:
            return ServerState.ABORT
        # self.logger.info('- New Order')
        return ServerState.OK

    def do_payment(self, w_id, d_id, c_w_id, c_d_id, c_query, h_amount):
        c_balance = 0
        c_ytd_payment = 0
        c_payment_cnt = 0
        c_credit = 'GC'
        c_id = 0
        if self._client.send_cmd("BEGIN;") == ServerState.ABORT:
            return ServerState.ABORT
        # self.logger.info('+ Payment')
        try:
            res = self._client.select(
                         table=WAREHOUSE,
                         col=(W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_YTD),
                         where=(W_ID, EQ, w_id))
            if res.state == ServerState.ABORT:
                return ServerState.ABORT
            w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd = res.data[0]
        except Exception as e:
            w_name, d_name = 'null', 'null'
        # w_ytd = eval(w_ytd)
        if self._client.update(
                  table=WAREHOUSE,
                  row=(W_YTD, W_YTD + '+' + str(h_amount)),
                  where=(W_ID, EQ, w_id)).state == ServerState.ABORT:
            return ServerState.ABORT
        try:
            res = self._client.select(
                         table=DISTRICT,
                         col=(D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_YTD),
                         where=[(D_W_ID, EQ, w_id), (D_ID, EQ, d_id)])
            if res.state == ServerState.ABORT:
                return ServerState.ABORT
            d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd = res.data[0]
        except Exception as e:
            d_name = 'null'
        # d_ytd = eval(d_ytd)
        if self._client.update(
                  table=DISTRICT,
                  row=(D_YTD, D_YTD + '+' + str(h_amount)),
                  where=[(D_W_ID, EQ, w_id), (D_ID, EQ, d_id)]).state == ServerState.ABORT:
            return ServerState.ABORT

        if type(c_query) == str:
            c_query = "'" + c_query + "'"
            try:
                result = self._client.select(
                                table=CUSTOMER,
                                col=(C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
                                     C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,
                                     C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT),
                                where=[(C_LAST, EQ, c_query),
                                       (C_W_ID, EQ, c_w_id),
                                       (C_D_ID, EQ, c_d_id)],
                                # order_by=C_FIRST,
                                # asc=True
                                )
                if result.state == ServerState.ABORT:
                    return ServerState.ABORT
                result = result.data[0]
            except Exception as e:
                c_credit = 'GC'
                c_id = 1
                c_balance = 0
                c_ytd_payment = 0
                c_payment_cnt = 0
        else:
            try:
                result = self._client.select(
                                table=CUSTOMER,
                                col=(C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
                                     C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,
                                     C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT),
                                where=[(C_ID, EQ, c_query),
                                       (C_W_ID, EQ, c_w_id),
                                       (C_D_ID, EQ, c_d_id)])
                if result.state == ServerState.ABORT:
                    return ServerState.ABORT
                result = result.data[0]
                c_id, c_first, c_midele, c_last, \
                    c_street_1, c_street_2, c_city, c_state, \
                    c_zip, c_phone, c_since, \
                    c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt = result  # result[len(result)//2]
                c_id = int(c_id)
                c_balance = float(c_balance)
                c_ytd_payment = float(c_ytd_payment)
                c_payment_cnt = int(c_payment_cnt)
            except Exception as e:
                c_credit = 'GC'
                c_id = 1
                c_balance = 0
                c_ytd_payment = 0
                c_payment_cnt = 0
        if self._client.update(
                  table=CUSTOMER,
                  row=[(C_BALANCE, c_balance + h_amount),
                       (C_YTD_PAYMENT, c_ytd_payment + 1),
                       (C_PAYMENT_CNT, c_payment_cnt + 1)],
                  where=[(C_W_ID, EQ, w_id), (C_D_ID, EQ, d_id), (C_ID, EQ, c_id)]).state == ServerState.ABORT:
            return ServerState.ABORT
        if c_credit == 'BC':
            try:
                c_data = (''.join(map(str, [c_id, c_d_id, c_w_id, d_id, h_amount]))
                          + self._client.select(
                                   table=CUSTOMER,
                                   col=(C_DATA,),
                                   where=[(C_ID, EQ, c_id),
                                          (C_W_ID, EQ, c_w_id),
                                          (C_D_ID, EQ, c_d_id)]).data[0][0])[0:500]
            except Exception as e:
                c_data = 'null'
            if self._client.update(
                      table=CUSTOMER,
                      row=(C_DATA, "'" + c_data + "'"),
                      where=[(C_W_ID, EQ, w_id), (C_D_ID, EQ, d_id), (C_ID, EQ, c_id)]).state == ServerState.ABORT:
                return ServerState.ABORT

        # 4 blank space
        h_data = w_name + '    ' + d_name
        if self._client.insert(
                  table=HISTORY,
                  rows=(c_id, c_d_id, c_w_id, d_id, w_id, "'" + current_time() + "'", h_amount,
                        "'" + h_data + "'")).state == ServerState.ABORT:
            return ServerState.ABORT

        if self._client.send_cmd("COMMIT;").state == ServerState.ABORT:
            return ServerState.ABORT
        # self.logger.info('- Payment')
        return ServerState.OK

    def do_order_status(self, w_id, d_id, c_query):
        c_id = 0
        if self._client.send_cmd("BEGIN;").state == ServerState.ABORT:
            return ServerState.ABORT
        # self.logger.info('+ Order Status')
        # 60% 执⾏
        if type(c_query) == str:
            c_query = "'" + c_query + "'"
            try:
                result = self._client.select(
                                table=CUSTOMER,
                                col=(COUNT(C_ID, "count_c_id"),),
                                where=[(C_LAST, EQ, c_query),
                                       (C_W_ID, EQ, w_id),
                                       (C_D_ID, EQ, d_id)])
                if result.state == ServerState.ABORT:
                    return ServerState.ABORT

                result = self._client.select(
                                table=CUSTOMER,
                                col=(C_BALANCE, C_FIRST, C_MIDDLE, C_LAST),
                                where=[(C_LAST, EQ, c_query),
                                       (C_W_ID, EQ, w_id),
                                       (C_D_ID, EQ, d_id)],
                                order_by=C_FIRST,
                                asc=True)
                if result.state == ServerState.ABORT:
                    return ServerState.ABORT
                result = result.data[0]
            except Exception as e:
                result = None
        else:
            try:
                result = self._client.select(
                                table=CUSTOMER,
                                col=(C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST),
                                where=[(C_ID, EQ, c_query),
                                       (C_W_ID, EQ, w_id),
                                       (C_D_ID, EQ, d_id)])
                if result.state == ServerState.ABORT:
                    return ServerState.ABORT
                result = result.data[0]
                c_id, c_balance, c_first, c_middle, c_last = result  # result[len(result)//2]
                c_id = int(c_id)
            except Exception as e:
                c_id = 2101
        try:
            res = self._client.select(
                         table=ORDERS,
                         col=(O_ID, O_ENTRY_D, O_CARRIER_ID),
                         where=[(O_W_ID, EQ, w_id),
                                (O_D_ID, EQ, d_id),
                                (O_C_ID, EQ, c_id)],
                         # order_by=O_ID
                         )

            if res.state == ServerState.ABORT:
                return ServerState.ABORT
            o_id, o_entry_id, o_carrier_id = res.data[0]
            o_id = int(o_id)
        except Exception as e:
            o_id = 1
        try:
            res = self._client.select(  # ol_i_id,ol_supply_w_id,ol_quantity,ol_amount,ol_delivery_d
                         table=ORDER_LINE,
                         col=(OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D),
                         where=[(OL_W_ID, EQ, w_id),
                                (OL_D_ID, EQ, d_id),
                                (OL_O_ID, EQ, o_id)])
            if res.state == ServerState.ABORT:
                return ServerState.ABORT
            result = res.data[0]
        except Exception as e:
            result = None
        if self._client.send_cmd("COMMIT;").state == ServerState.ABORT:
            return ServerState.ABORT
        # self.logger.info('- Order Status')
        return ServerState.OK

    def do_delivery(self, w_id, o_carrier_id):
        t1 = time.time()
        if self._client.send_cmd("BEGIN;").state == ServerState.ABORT:
            return ServerState.ABORT
        # self.logger.info('+ Delivery')
        # dat = q.get()
        # w_id = dat['w_id']
        # o_carrier_id = dat['o_carrier_id']
        for d_id in range(1, 11):
            try:
                res = self._client.select(
                             table=NEW_ORDERS,
                             col=(MIN(NO_O_ID),),
                             where=[(NO_W_ID, EQ, w_id), (NO_D_ID, EQ, d_id)],
                             # order_by=NO_O_ID,
                             # asc=True
                             )
                if res.state == ServerState.ABORT:
                    return ServerState.ABORT
                o_id = int(res.data[0][0])
            except Exception as e:
                o_id = 2101
            if self._client.delete(
                      table=NEW_ORDERS,
                      where=[(NO_W_ID, EQ, w_id), (NO_D_ID, EQ, d_id), (NO_O_ID, EQ, o_id)]).state == ServerState.ABORT:
                return ServerState.ABORT
            try:
                res = self._client.select(
                             table=ORDERS,
                             col=(O_C_ID,),
                             where=[(O_ID, EQ, o_id), (O_W_ID, EQ, w_id), (O_D_ID, EQ, d_id)])
                if res.state == ServerState.ABORT:
                    return ServerState.ABORT
                o_c_id = res.data[0][0]
                o_c_id = int(o_c_id)
            except Exception as e:
                o_c_id = 2101
            if self._client.update(
                      table=ORDERS,
                      row=(O_CARRIER_ID, o_carrier_id),
                      where=[(O_ID, EQ, o_id), (O_W_ID, EQ, w_id), (O_D_ID, EQ, d_id)]).state == ServerState.ABORT:
                return ServerState.ABORT
            try:
                res = self._client.select(
                             table=ORDER_LINE,
                             where=[(OL_W_ID, EQ, w_id), (OL_D_ID, EQ, d_id), (OL_O_ID, EQ, o_id)])
                if res.state == ServerState.ABORT:
                    return ServerState.ABORT
                order_lines = res
                if not order_lines:
                    order_lines = []
                else:
                    order_lines = [i.strip('|').split('|').strip('- ') for i in order_lines[1:]]

            except Exception as e:
                order_lines = []

            try:
                res = self._client.select(
                             table=ORDER_LINE,
                             col=(SUM(OL_AMOUNT),),
                             where=[(OL_W_ID, EQ, w_id), (OL_D_ID, EQ, d_id), (OL_O_ID, EQ, o_id)])
                if res.state == ServerState.ABORT:
                    return ServerState.ABORT
                ol_amount = res.data[0][0]
                if not ol_amount:
                    ol_amount = 0
                else:
                    ol_amount = [float(o[0]) for o in ol_amount]
                    ol_amount = sum(ol_amount)
            except Exception as e:
                ol_amount = 0

            for line in order_lines:
                if line[0] == '':
                    continue
                if self._client.update(
                          table=ORDER_LINE,
                          row=(OL_DELIVERY_D, "'" + current_time() + "'"),
                          where=[(OL_W_ID, EQ, w_id), (OL_D_ID, EQ, d_id),
                                 (OL_O_ID, EQ, eval(line[0]))]).state == ServerState.ABORT:
                    return ServerState.ABORT
            try:
                res = self._client.select(
                             table=CUSTOMER,
                             col=(C_BALANCE, C_DELIVERY_CNT),
                             where=[(C_W_ID, EQ, w_id), (C_D_ID, EQ, d_id), (C_ID, EQ, o_c_id)])
                if res.state == ServerState.ABORT:
                    return ServerState.ABORT
                c_balance, c_delivery_cnt = res.data[0]
                c_balance = float(c_balance)
                c_delivery_cnt = int(c_delivery_cnt)
            except Exception as e:
                c_balance = 0
                c_delivery_cnt = 0
            # self.logger.info(c_balance, ol_amount, c_delivery_cnt)
            if self._client.update(
                      table=CUSTOMER,
                      row=[(C_BALANCE, c_balance + ol_amount), (C_DELIVERY_CNT, c_delivery_cnt + 1)],
                      where=[(C_W_ID, EQ, w_id), (C_D_ID, EQ, d_id), (C_ID, EQ, o_c_id)]).state == ServerState.ABORT:
                return ServerState.ABORT
        if self._client.send_cmd("COMMIT;").state == ServerState.ABORT:
            return ServerState.ABORT
        t2 = time.time()
        # put_txn(lock,Delivery,t2-t1,True)
        # self.logger.info('- Delivery')

        self._delivery_stop = True
        return ServerState.OK

    def do_stock_level(self, w_id, d_id, level):
        if self._client.send_cmd("BEGIN;").state == ServerState.ABORT:
            return ServerState.ABORT
        # self.logger.info('+ Stock Level')
        try:
            res = self._client.select(
                         table=DISTRICT,
                         col=(D_NEXT_O_ID,),
                         where=[(D_W_ID, EQ, w_id), (D_ID, EQ, d_id)])
            if res.state == ServerState.ABORT:
                return ServerState.ABORT
            d_next_o_id = int(res.data[0][0])
        except Exception as e:
            d_next_o_id = 0
        # self.logger.info("d_next_o_id", d_next_o_id)
        try:
            res = self._client.select(
                         table=ORDER_LINE,
                         where=[(OL_W_ID, EQ, w_id),
                                (OL_D_ID, EQ, d_id),
                                (OL_O_ID, LT, d_next_o_id - 20),
                                (OL_O_ID, LT, d_next_o_id)])
            if res.state == ServerState.ABORT:
                return ServerState.ABORT
            order_lines = res
            # self.logger.info(order_lines)
            items = set([int(order_line[5]) for order_line in order_lines])
            # self.logger.info(items)
        except Exception as e:
            # 结果为空，到这里
            items = []
        low_stock = 0
        for item in items:
            try:
                res = self._client.select(
                             table=STOCK,
                             col=(S_QUANTITY,),
                             where=[(S_I_ID, EQ, item),
                                    (S_W_ID, EQ, w_id),
                                    (S_QUANTITY, LT, level)])
                if res.state == ServerState.ABORT:
                    return ServerState.ABORT
                cur_quantity = int(res.data[0][0])
            except Exception as e:
                cur_quantity = 0
            # low_stock += eval(cur_quantity)
        # low_stock = self._client.select(
        #                     table=STOCK,
        #                     col=S_QUANTITY,
        #                     where=[(S_W_ID,eq,w_id),(S_I_ID,eq,ol_i_id),(S_QUANTITY,lt,level)])
        if self._client.send_cmd("COMMIT;").state == ServerState.ABORT:
            return ServerState.ABORT
        # self.logger.info('- Stock Level')
        return ServerState.OK
