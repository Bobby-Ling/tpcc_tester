import pathlib
import random
import string
import os
import sys

file_path = pathlib.Path(__file__)
file_dir = file_path.parent
from tpcc_tester.client.base import DBClient
from tpcc_tester.client.rmdb_client import RMDBClient, ServerState, Result

class IndexFuzzer:
    TABLE_NAME = "fuzz_test"
    INDEXED_COLS = ('val_a', 'val_b')

    def __init__(self):
        self.client = RMDBClient()
        # 黄金模型: 使用 set 存储唯一索引键的元组, 用于快速冲突检测
        self.golden_model_keys = set()
        # 数据镜像: 使用 dict 存储完整数据, 主键是 id, 便于更新和删除
        self.data_mirror = {}
        self.next_id = 1

    def connect(self):
        assert self.client.connect() == ServerState.OK, "Failed to connect to RMDB server."

    def _execute_sql(self, sql: str, expect_fail: bool = False) -> Result:
        print(f"Executing (expect={'FAIL' if expect_fail else 'OK'}): {sql}")

        result = self.client.send_cmd(sql)

        if expect_fail:
            if result.state == ServerState.OK:
                print("Expected: FAILED ")
                print(f"Actual:   SUCCEEDED, {result}")
                raise AssertionError("Expected failure, but got success.")
        else: # expect success
            if result.state != ServerState.OK:
                print("Expected: SUCCEEDED")
                print(f"Actual:   FAILED with state '{result.state}', {result}")
                raise AssertionError("Expected success, but got failure.")

        return result

    def setup_test_table(self):
        print("\n--- Setting up test table ---")
        try:
            self._action_drop_index()
            self.client.send_cmd(f"drop table {self.TABLE_NAME};")
        except Exception as e:
            pass

        # 创建新表
        create_table_sql = f"""
        create table {self.TABLE_NAME} (
            id int,
            val_a int,
            val_b char(8)
        );
        """
        self._execute_sql(create_table_sql)

        # 创建唯一索引
        self._action_create_index()

        # 重置内部状态
        self.golden_model_keys = set()
        self.data_mirror = {}
        self.next_id = 1
        print("Setup complete.")

    def _generate_random_values(self) -> dict:
        """生成一组随机的 val_a 和 val_b."""
        return {
            'val_a': random.randint(0, 100), # 缩小范围以增加冲突概率
            'val_b': ''.join(random.choices(string.ascii_lowercase, k=8))
        }

    @DBClient.ignore_exception
    def _action_create_index(self):
        index_cols_str = ','.join(self.INDEXED_COLS)
        create_index_sql = f"create index {self.TABLE_NAME}({index_cols_str});"
        self._execute_sql(create_index_sql)

    @DBClient.ignore_exception
    def _action_drop_index(self):
        index_cols_str = ','.join(self.INDEXED_COLS)
        drop_index_sql = f"drop index {self.TABLE_NAME}({index_cols_str});"
        self._execute_sql(drop_index_sql)

    def _action_recreate_index(self):
        print("\nAction: RECREATE INDEX")
        self._action_drop_index()
        self._action_create_index()

    def _action_insert(self):
        print("\nAction: INSERT")
        new_values = self._generate_random_values()
        new_key = tuple(new_values[col] for col in self.INDEXED_COLS)

        # 预测: 检查黄金模型, 看这次插入是否会违反唯一约束
        should_fail = new_key in self.golden_model_keys

        sql = f"insert into {self.TABLE_NAME} values ({self.next_id}, {new_values['val_a']}, '{new_values['val_b']}');"
        self._execute_sql(sql, expect_fail=should_fail)

        # 如果操作成功(且本应成功), 更新我们的模型
        if not should_fail:
            self.golden_model_keys.add(new_key)
            self.data_mirror[self.next_id] = {
                'id': self.next_id,
                **new_values
            }
            self.next_id += 1

    def _action_update(self):
        print("\nAction: UPDATE")
        if not self.data_mirror:
            print("Skipping UPDATE: table is empty.")
            return

        # 随机选择一行进行更新
        target_id = random.choice(list(self.data_mirror.keys()))
        target_row = self.data_mirror[target_id]

        new_values = self._generate_random_values()
        new_key = tuple(new_values[col] for col in self.INDEXED_COLS)

        # 预测: 新的键是否与其他行的键冲突
        # 我们需要从黄金模型中临时移除当前行的键再做判断
        original_key = tuple(target_row[col] for col in self.INDEXED_COLS)

        should_fail = False
        if new_key != original_key:
            if new_key in self.golden_model_keys:
                should_fail = True

        sql = f"update {self.TABLE_NAME} set val_a = {new_values['val_a']}, val_b = '{new_values['val_b']}' where id = {target_id};"
        self._execute_sql(sql, expect_fail=should_fail)

        # 如果操作成功(且本应成功), 更新我们的模型
        if not should_fail:
            self.golden_model_keys.remove(original_key)
            self.golden_model_keys.add(new_key)
            self.data_mirror[target_id].update(new_values)

    def _action_delete(self):
        print("\nAction: DELETE")
        if not self.data_mirror:
            print("Skipping DELETE: table is empty.")
            return

        # 随机选择一行进行删除
        target_id = random.choice(list(self.data_mirror.keys()))
        target_row = self.data_mirror[target_id]

        sql = f"delete from {self.TABLE_NAME} where id = {target_id};"
        # 删除操作不应该因为唯一约束而失败
        self._execute_sql(sql, expect_fail=False)

        # 更新模型
        original_key = tuple(target_row[col] for col in self.INDEXED_COLS)
        self.golden_model_keys.remove(original_key)
        del self.data_mirror[target_id]

    def validate_final_state(self):
        print("\n--- Final State Validation ---")
        sql = f"select id, val_a, val_b from {self.TABLE_NAME} order by id;"
        result = self._execute_sql(sql)

        db_data = result.data

        # row_sort
        golden_data = sorted(list(self.data_mirror.values()), key=lambda x: x['id'])

        parsed_db_data = []
        for row in db_data:
            try:
                parsed_db_data.append({
                    'id': int(row[0]),
                    'val_a': int(row[1]),
                    'val_b': str(row[2]).strip()
                })
            except (ValueError, IndexError) as e:
                print(f"Failed to parse database row: {row}. Error: {e}")
                raise

        # 比较行数
        if len(parsed_db_data) != len(golden_data):
            print(f"Expected (Golden Model): {len(golden_data)} rows")
            print(f"Actual (Database):       {len(parsed_db_data)} rows")
            raise AssertionError("Row count mismatch.")

        # 逐行逐字段比较
        for db_row, golden_row in zip(parsed_db_data, golden_data):
            if db_row != golden_row:
                print(f"Expected (Golden Model): {golden_row}")
                print(f"Actual (Database):       {db_row}")
                raise AssertionError("Data mismatch.")

        print("Final state validation successful. Database is consistent with the model.")

    def run_fuzz_cycle(self, iterations: int):
        """运行N次随机操作."""
        self.connect()
        self.setup_test_table()

        actions = [
            self._action_insert,
            self._action_update,
            self._action_delete,
            self._action_drop_index,
            self._action_create_index,
            self._action_recreate_index,
        ]
        weights = [0.4, 0.3, 0.2, 0.02, 0.02, 0.06]

        for i in range(iterations):
            # 根据权重随机选择一个操作
            action = random.choices(actions, weights=weights, k=1)[0]
            try:
                action()
            except AssertionError:
                print("\n!!! Fuzzing stopped due to a bug. !!!")
                return

        # 所有迭代完成后, 进行最终的状态校验
        try:
            self.validate_final_state()
            print(f"\nFuzzing completed successfully for {iterations} iterations!")
        except AssertionError:
            print("\n!!! Fuzzing failed during final validation. !!!")

    def teardown(self):
        print("\n--- Tearing down ---")
        self._action_drop_index()
        self.client.send_cmd(f"drop table {self.TABLE_NAME};")
        self.client.close()
        print("Teardown complete.")


if __name__ == '__main__':
    random.seed(42)

    num_iterations = 50000

    fuzzer = IndexFuzzer()
    try:
        fuzzer.run_fuzz_cycle(iterations=num_iterations)
    finally:
        fuzzer.teardown()
