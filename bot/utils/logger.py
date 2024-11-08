import sys
import re
from loguru import logger


def formatter(record, format_string):
    return format_string + record["extra"].get("end", "\n") + "{exception}"


def clean_brackets(raw_str):
    return re.sub(r'<.*?>', '', raw_str)


def logging_setup():
    format_info = "<white>{time:YYYY-MM-DD HH:mm:ss}</white> | <level>{level}</level> | <cyan><b>{line}</b></cyan> | <white>{message}</white>"
    format_error = "<white>{time:YYYY-MM-DD HH:mm:ss.SS}</white> | <level>{level}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | <white>{message}</white>"
    logger_path = r"logs/out.log"

    logger.remove()

    logger.add(logger_path, colorize=True, format=lambda record: formatter(record, clean_brackets(format_error)), rotation="100 MB")
    logger.add(sys.stdout, colorize=True, format=lambda record: formatter(record, format_info), level="INFO")

logger = logger.opt(colors=True)

logging_setup()
