# 测试框架思路

服务器状态:
ServerState{OK, DOWN, ABORT}

返回结果:
Result{state: ServerState, result: List[List[Any]], result_str: str}

接口: 
- DBClient(db: str, port: int)
    - connect() -> ServerState{OK, DOWN, ABORT} # 执行成功/服务器已经关闭或已退出/执行失败(可能是唯一性约束失败或写写冲突等)
    - create_table() -> Result
    - create_index() -> Result
    - drop_table() -> Result
    - drop_index() -> Result
    - begin() -> Result
    - abort() -> Result
    - commit() -> Result
    - select() -> Result
    - insert() -> Result
    - update() -> Result
    - delete() -> Result
    - crash() -> Result
    - send_tcl() -> Result
    - send_ddl() -> Result
    - send_dml() -> Result
    - send_dql() -> Result
    - send_cmd() -> Result # 底层函数, 由send_ddl等调用
    - append_record(row_sort = True) -> None # 仅基类实现, send_cmd调用此来记录sql和对应output, 可以使用logging实现高性能记录

create_table~delete基类提供默认实现(调用send_*函数)

实现: 
- RMDBClient(DBClient)
- MySQLClient(DBClient)
- SLTClient(DBClient), 用于将sql语句记录至sqllogictest的slt文件, 其中send_tcl/send_ddl/send_dml使用statement ok, send_dql使用query T+, 不需要记录结果
- SQLClient(DBClient), 用于将sql语句记录至sql文件

接口:
- DBDriver(db: str, port: int)      
    - clients: List[DBClient]
    - ref_client: DBClient 用于比对
    - load_data(sql_file: Path)

实现: 
- RMDBDriver(DBDriver)
- MySQLDriver(DBDriver)

接口:
- TestDriver()
    - load_data()
    - load_data_from_csv()
    - run_test()

实现:
- PerfTestDriver(TestDriver)
- RecoveryTestDriver(TestDriver)

PerfTestDriver是原TPCC-Tester仓库(test/TPCC-Tester)中(test/TPCC-Tester/mysql/driver.py)的逻辑;
RecoveryTestDriver是TPCC-Tester-Recovery-Copy仓库(test/TPCC-Tester-Recovery-Copy)中(test/TPCC-Tester-Recovery-Copy/crash_recovery_tester.py)的逻辑

