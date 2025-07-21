from functools import wraps
import logging
import colorlog
from dataclasses import dataclass
from enum import Enum
from typing import List, Any, Optional
from pathlib import Path

class ServerState(Enum):
    OK = "OK"
    DOWN = "DOWN"
    ABORT = "ABORT"
    ERROR = "ERROR"


@dataclass
class Result:
    state: ServerState
    # 表名
    metadata: List[str]
    # 数据
    data: List[List[Any]]
    result_str: str
    raw: Any = None
def run_once(f):
    """Runs a function (successfully) only once.
    The running can be reset by setting the `has_run` attribute to False
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            result = f(*args, **kwargs)
            wrapper.has_run = True
            return result
    wrapper.has_run = False
    return wrapper


def setup_console_handler(level: int = logging.INFO, console_formatter: Optional[str] = None):
    # logging.warning(f"setup_console_handler, level={level}, console_formatter={console_formatter}")
    console_handler = colorlog.StreamHandler()
    console_handler.setLevel(level)

    # [18:27:23.947][INFO][MySQLClient][mysql_client.py:50][connect] Connected to MySQL at localhost:3306/tpcc_test
    # 类似spdlog的格式: [%T.%e][%^%l%$][%n][%s:%#][%!] %v
    # %T.%e -> 时间.毫秒
    # %^%l%$ -> 带颜色的日志级别
    # %n -> 模块名
    # %s:%# -> 文件名:行号
    # %! -> 函数名
    # %v -> 消息内容
    if console_formatter is None:
        console_formatter = colorlog.ColoredFormatter(
            '[%(log_color)s%(asctime)s.%(msecs)03d%(reset)s][%(log_color)s%(levelname)s%(reset)s][%(name)s][%(filename)s:%(lineno)d][%(funcName)s] %(message)s',
            datefmt='%H:%M:%S',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white',
            },
            secondary_log_colors={},
            style='%'
        )
    else:
        console_formatter = colorlog.ColoredFormatter(console_formatter)
    console_handler.setFormatter(console_formatter)
    return console_handler

def setup_file_handler(log_file: str, level: int = logging.DEBUG, file_formatter: Optional[str] = None):
    # logging.warning(f"setup_file_handler, log_file={log_file}, level={level}, file_formatter={file_formatter}")
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    # 文件handler, 不带颜色
    file_handler = logging.FileHandler(log_file, mode='w')
    if file_formatter is None:
        file_formatter = logging.Formatter(
            '[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s][%(filename)s:%(lineno)d][%(funcName)s] %(message)s',
            datefmt='%H:%M:%S'
        )
    else:
        file_formatter = logging.Formatter(file_formatter)
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(level)
    return file_handler

@run_once
def setup_global_logging():
    logging.root.handlers.clear()
    console_handler = setup_console_handler(logging.CRITICAL)
    logging.root.addHandler(console_handler)
    file_handler = setup_file_handler("logs/global.log", logging.DEBUG)
    logging.root.addHandler(file_handler)

setup_global_logging()

def setup_logging(logger_name: str, propagate=True, console_level: int = logging.INFO, file_level: int = logging.DEBUG, console_formatter: Optional[str] = None, file_formatter: Optional[str] = None, log_file: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger(logger_name)

    if not logger.handlers:
        # https://docs.python.org/zh-cn/3.12/library/logging.html
        # foo.bar 默认会传递给 foo
        logger.propagate = propagate
        # logging.fatal(f"adding {logger_name}, propagate={propagate}, console_level={console_level}, file_level={file_level}, console_formatter={console_formatter}, file_formatter={file_formatter}, log_file={log_file}")

        console_handler = setup_console_handler(console_level, console_formatter)

        logger.addHandler(console_handler)

        if log_file is None:
            log_file = f"{logger_name}.log"

        # 文件handler, 不带颜色
        file_handler = setup_file_handler(f"logs/{log_file}", file_level, file_formatter)
        logger.addHandler(file_handler)

        logger.setLevel(min(console_level, file_level))

    return logger