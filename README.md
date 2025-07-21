# tpcc_tester

插入数据

```sh
python tpcc_tester/runner.py --prepare --client=rmdb
# python tpcc_tester/runner.py --prepare --client=mysql
```

使用已有的数据

```sh
python tpcc_tester/runner.py --thread 1 --ro 10 --analyze --client=rmdb
```

插入数据并运行事务

```sh
python tpcc_tester/runner.py --prepare --thread 8 --ro 100 --rw 50 --analyze --client=rmdb
```

```sh
usage: runner.py [-h] [--prepare] [--analyze] [--clean] [--rw RW]
                 [--ro RO] [--thread THREAD]
                 [--client {rmdb,mysql,slt,sql}]

Python Script with Thread Number Argument

options:
  -h, --help            show this help message and exit
  --prepare             Enable prepare mode
  --analyze             Enable analyze mode
  --clean               Clean database(execlude with other options)
  --rw RW               Read write transaction phase time
  --ro RO               Read only transaction phase time
  --thread THREAD       Thread number
  --client {rmdb,mysql,slt,sql}
                        Client type
```