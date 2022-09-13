import logging
from typing import Union


def str2bool(input_arg: Union[str, bool]) -> bool:
    if isinstance(input_arg, bool):
        return input_arg
    if input_arg.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif input_arg.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise logger.error("Boolean value is expected.")


class CustomFormatter(logging.Formatter):

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


logger = logging.getLogger("funda_scraper")
logger.setLevel(logging.INFO)

# # create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

ch.setFormatter(CustomFormatter())

logger.addHandler(ch)
