
# CNT_W = 50
# CNT_ITEM = 100000
# CNT_STOCK = CNT_W * 100000
# CNT_DISTRICT = CNT_W * 10
# CNT_CUSTOMER = CNT_W * 10 * 3000
# CNT_HISTORY = CNT_W * 10 * 3000
# CNT_ORDERS = CNT_W * 10 * 3000
# CNT_NEW_ORDERS = CNT_W * 10 * 900
# # CNT_ORDER_LINE = CNT_ORDERS * 10
# CNT_ORDER_LINE = 15001487

CNT_W = 2
CNT_ITEM = 100000
CNT_STOCK = CNT_W * 100000
CNT_DISTRICT = CNT_W * 10
CNT_CUSTOMER = CNT_W * 10 * 3000
CNT_HISTORY = CNT_W * 10 * 3000
CNT_ORDERS = CNT_W * 10 * 3000
CNT_NEW_ORDERS = CNT_W * 10 * 900
# CNT_ORDER_LINE = CNT_ORDERS * 10
CNT_ORDER_LINE = 600320

DATA_MAX = 255

"""
wc -l tpcc_csv/*.csv
    60001 tpcc_csv/customer.csv
       21 tpcc_csv/district.csv
    60001 tpcc_csv/history.csv
   100001 tpcc_csv/item.csv
    18001 tpcc_csv/new_orders.csv
   600321 tpcc_csv/order_line.csv
    60001 tpcc_csv/orders.csv
   200001 tpcc_csv/stock.csv
        3 tpcc_csv/warehouse.csv
  1098351 总计

wc -l tpcc_csv_no_header/*.csv
    60000 tpcc_csv_no_header/customer.csv
       20 tpcc_csv_no_header/district.csv
    60000 tpcc_csv_no_header/history.csv
   100000 tpcc_csv_no_header/item.csv
    18000 tpcc_csv_no_header/new_orders.csv
   600320 tpcc_csv_no_header/order_line.csv
    60000 tpcc_csv_no_header/orders.csv
   200000 tpcc_csv_no_header/stock.csv
        2 tpcc_csv_no_header/warehouse.csv
  1098342 总计

wc -l tpcc_sql/*.sql
    60000 tpcc_sql/customer.sql
       20 tpcc_sql/district.sql
    60000 tpcc_sql/history.sql
   100000 tpcc_sql/item.sql
    18000 tpcc_sql/new_orders.sql
   600320 tpcc_sql/order_line.sql
    60000 tpcc_sql/orders.sql
   200000 tpcc_sql/stock.sql
        2 tpcc_sql/warehouse.sql
  1098342 总计

"""