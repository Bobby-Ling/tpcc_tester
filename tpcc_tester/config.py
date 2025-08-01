import argparse
from dataclasses import dataclass

# CNT_W = 50
# CNT_ITEM = 100000
# CNT_STOCK = CNT_W * 100000
# CNT_DISTRICT = CNT_W * 10
# CNT_CUSTOMER = CNT_W * 10 * 3000
# CNT_HISTORY = CNT_W * 10 * 3000
# CNT_ORDERS = CNT_W * 10 * 3000
# CNT_NEW_ORDERS = CNT_W * 10 * 900
# CNT_ORDER_LINE = CNT_ORDERS * 10
# CNT_ORDER_LINE = 15001487


# CNT_W = 2
# CNT_ITEM = 100000
# CNT_STOCK = CNT_W * 100000
# CNT_DISTRICT = CNT_W * 10
# CNT_CUSTOMER = CNT_W * 10 * 3000
# CNT_HISTORY = CNT_W * 10 * 3000
# CNT_ORDERS = CNT_W * 10 * 3000
# CNT_NEW_ORDERS = CNT_W * 10 * 900
# CNT_ORDER_LINE = CNT_ORDERS * 10
# CNT_ORDER_LINE = 600320

# DATA_MAX = 50

"""
CONFIG_NUM_WARE = 2
    60001 tpcc_csv/customer.csv
       21 tpcc_csv/district.csv
    60001 tpcc_csv/history.csv
   100001 tpcc_csv/item.csv
    18001 tpcc_csv/new_orders.csv
   600001 tpcc_csv/order_line.csv
    60001 tpcc_csv/orders.csv
   200001 tpcc_csv/stock.csv
        3 tpcc_csv/warehouse.csv
  1098031 总计
    60000 tpcc_csv_no_header/customer.csv
       20 tpcc_csv_no_header/district.csv
    60000 tpcc_csv_no_header/history.csv
   100000 tpcc_csv_no_header/item.csv
    18000 tpcc_csv_no_header/new_orders.csv
   600000 tpcc_csv_no_header/order_line.csv
    60000 tpcc_csv_no_header/orders.csv
   200000 tpcc_csv_no_header/stock.csv
        2 tpcc_csv_no_header/warehouse.csv
  1098022 总计

"""

@dataclass
class Config:
    prepare: bool = False
    analyze: bool = False
    validate: bool = True
    clean: bool = False
    rw: int = 0
    ro: int = 0
    thread_num: int = 0
    client_type: 'ClientType' = None
    seed: int = 42
    warehouse: int = 50
    disable_logging: bool = False

    CNT_W = warehouse
    CNT_ITEM = 100000
    CNT_STOCK = CNT_W * 100000
    CNT_DISTRICT = CNT_W * 10
    CNT_CUSTOMER = CNT_W * 10 * 3000
    CNT_HISTORY = CNT_W * 10 * 3000
    CNT_ORDERS = CNT_W * 10 * 3000
    CNT_NEW_ORDERS = CNT_W * 10 * 900
    CNT_ORDER_LINE = CNT_ORDERS * 10

    W_ID_MAX = CNT_W + 1
    D_ID_MAX = 11

    DATA_MAX = 50

    def __post_init__(self):
        self.parse()

    def parse(self):
        parser = argparse.ArgumentParser(description='Python Script with Thread Number Argument')
        parser.add_argument('-p', '--prepare', action='store_true', help='Enable prepare mode')
        parser.add_argument('-a', '--analyze', action='store_true', help='Enable analyze mode')
        parser.add_argument('-n', '--no-validate', action='store_true', help='Disable consistency check')
        parser.add_argument('-c', '--clean', action='store_true', help='Clean database(execlude with other options)')
        parser.add_argument('--rw', type=int, help='Read write transaction phase time')
        parser.add_argument('--ro', type=int, help='Read only transaction phase time')
        parser.add_argument('-t', '--thread', type=int, help='Thread number')
        parser.add_argument('-ct', '--client', type=str, default='rmdb', choices=['rmdb', 'mysql', 'slt', 'sql'], help='Client type')
        parser.add_argument('-s', '--seed', type=int, default=42, help='Random seed')
        parser.add_argument('-w', '--warehouse', type=int, default=50, help='Warehouse number')
        parser.add_argument('-l', '--disable-logging', action='store_true', help='Disable logging')

        from tpcc_tester.client.base import ClientType

        args = parser.parse_args()
        self.prepare: bool = args.prepare or self.prepare
        self.analyze: bool = args.analyze or self.analyze
        self.validate: bool = not args.no_validate and self.validate
        self.clean: bool = args.clean or self.clean
        self.rw: int = args.rw or self.rw
        self.ro: int = args.ro or self.ro
        self.thread_num: int = args.thread or self.thread_num
        self.client_type = ClientType(args.client) or self.client_type
        self.seed: int = args.seed or self.seed
        self.warehouse: int = args.warehouse or self.warehouse
        self.disable_logging: bool = args.disable_logging or self.disable_logging

        self.CNT_W = self.warehouse
        self.CNT_ITEM = 100000
        self.CNT_STOCK = self.CNT_W * 100000
        self.CNT_DISTRICT = self.CNT_W * 10
        self.CNT_CUSTOMER = self.CNT_W * 10 * 3000
        self.CNT_HISTORY = self.CNT_W * 10 * 3000
        self.CNT_ORDERS = self.CNT_W * 10 * 3000
        self.CNT_NEW_ORDERS = self.CNT_W * 10 * 900
        self.CNT_ORDER_LINE = self.CNT_ORDERS * 10

        self.W_ID_MAX = self.CNT_W + 1
        self.D_ID_MAX = 11
