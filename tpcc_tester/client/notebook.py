# %%
import pathlib
import sys
import os
import pandas as pd

file_path = pathlib.Path(__file__)
file_dir = file_path.parent
sys.path.append(str(file_dir.parent.parent))

from tpcc_tester.client import RMDBClient

# %%
os.environ["RMDB_PORT"] = "7005"
client = RMDBClient(port=7005)
client.connect()

warehouse_df = pd.DataFrame(client.send_cmd("select * from warehouse;").data)
district_df = pd.DataFrame(client.send_cmd("select * from district;").data)
customer_df = pd.DataFrame(client.send_cmd("select * from customer;").data)
new_orders_df = pd.DataFrame(client.send_cmd("select * from new_orders;").data)
orders_df = pd.DataFrame(client.send_cmd("select * from orders;").data)
order_line_df = pd.DataFrame(client.send_cmd("select * from order_line;").data)
item_df = pd.DataFrame(client.send_cmd("select * from item;").data)
stock_df = pd.DataFrame(client.send_cmd("select * from stock;").data)

# %%

result = client.send_cmd("select * from new_orders;")
df = pd.DataFrame(result.data)
print(len(df) - len(df.drop_duplicates()))
duplicate_indices = df[df.duplicated(keep=False)].index.tolist()
print(df.iloc[duplicate_indices[0], :])

# %%