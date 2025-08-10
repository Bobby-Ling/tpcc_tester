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
os.environ["RMDB_PORT"] = "7003"
client = RMDBClient(port=7003)
client.connect()
result = client.send_cmd("select * from new_orders;")
df = pd.DataFrame(result.data)
print(len(df) - len(df.drop_duplicates()))

# %%