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

def setup_logging(logger_name: str, console_level: int = logging.INFO, file_level: int = logging.DEBUG, console_formatter: Optional[str] = None, file_formatter: Optional[str] = None, log_file: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    # 防止传播到父logger
    logger.propagate = False

    if not logger.handlers:
        console_handler = colorlog.StreamHandler()

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
        console_handler.setLevel(console_level)
        # logging.warning(f"adding console_formatter: {console_formatter}")
        logger.addHandler(console_handler)

        if log_file is None:
            log_file = f"{logger_name}.log"

        log_file = f'logs/{log_file}'
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
        file_handler.setLevel(file_level)
        # logging.warning(f"adding file_formatter: {file_formatter}")
        logger.addHandler(file_handler)

        logger.setLevel(min(console_level, file_level))

    return logger