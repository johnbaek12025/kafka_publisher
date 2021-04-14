"""
Library of components for the ThinkPoll SKM implementation.
"""

__version__ = "1.0.0"
__pkgname__ = "kpm"

import locale
import logging
import os
import platform
import pickle
import sys
import signal
import regex as re

from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler


logger = logging.getLogger(__name__)


def platform_info():
    lines = []
    lines.append("KPM: %s" % __version__)
    lines.append("Python: %s" % sys.version.replace("\n", " "))
    lines.append("Host: %s" % platform.node())
    lines.append("Platform: %s" % platform.platform())
    lines.append("Locale: %s" % locale.setlocale(locale.LC_ALL))
    return lines


def setup_logging(
    appname=None,
    appvers=None,
    debug=None,
    filename=None,
    dirname=None,
    max_bytes=None,
    backup_count=None,
    interval=None,
    log_dict=dict(),
    emit_platform_info=False,
):
    """Provide a sane set of defaults for logging.

    directory - where to put log files, current dir if nothing specified
    filename - the base name to use for the log file, appname if not specified
    max_bytes - when the log exceeds this size, the log will rotate
    interval - time in minutes
    backup_count - maximum number of files to retain

    Configure a rotating log file that rotates when the file size exceeds a
    specified number of bytes or when the time exceeds the specified interval.
    Then naming of the rotated files uses this pattern:

    filename.log
    filename.log.YYYY-mm-dd_HH-MM
    """

    # start with values from the dict, override with any specific arguments
    if debug is None:
        debug = to_bool(log_dict.get("debug", False))
    if filename is None:
        filename = log_dict.get("filename")
    if dirname is None:
        dirname = log_dict.get("directory")
    if max_bytes is None:
        max_bytes = int(log_dict.get("max_bytes", 10 * 1024 * 1024))  # 10 MB
    if interval is None:
        interval = int(log_dict.get("interval", 1440))  # 1 day
    if backup_count is None:
        backup_count = int(log_dict.get("backup_count", 30))

    # set the log level
    level = logging.DEBUG if debug else logging.INFO
    # if a log dir was specified, use it.  default to appname if no filename
    # was specified.
    if dirname is not None:
        create_dir(dirname)
        if filename is None and appname is not None:
            filename = "%s.log" % appname
        if filename is not None:
            filename = os.path.join(dirname, filename)

    # otherwise, if a filename was specified, use it.  if not, we go to stdout.
    if filename is None:
        hand = logging.StreamHandler()
    else:
        hand = EnhancedRotatingFileHandler(
            filename=filename,
            when="M",
            interval=interval,
            maxBytes=max_bytes,
            backupCount=backup_count,
        )

    # fmt = '%(asctime)s.%(msecs)03d %(processName)s %(threadName)s %(name)s %(funcName)s: %(message)s' \
    #     if level == logging.DEBUG else '%(asctime)s.%(msecs)03d %(processName)s %(threadName)s %(message)s'

    fmt = (
        "%(asctime)s.%(msecs)03d %(processName)s %(threadName)s %(name)s %(funcName)s: %(message)s"
        if level == logging.DEBUG
        else "[%(asctime)s.%(msecs)03d][%(processName)s %(threadName)s][%(filename)s:%(lineno)d] %(message)s"
    )

    datefmt = "%Y.%m.%d %H:%M:%S"
    hand.setFormatter(logging.Formatter(fmt, datefmt))

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers = []
    root_logger.addHandler(hand)

    if emit_platform_info:
        if appname and appvers:
            logger.info("%s: %s" % (appname, appvers))
        for line in platform_info():
            logger.info(line)


class EnhancedRotatingFileHandler(TimedRotatingFileHandler, RotatingFileHandler):
    def __init__(
        self,
        filename,
        mode="a",
        maxBytes=0,
        backupCount=0,
        encoding=None,
        delay=0,
        when="h",
        interval=1,
        utc=False,
    ):
        TimedRotatingFileHandler.__init__(
            self, filename, when, interval, backupCount, encoding, delay, utc
        )
        RotatingFileHandler.__init__(
            self, filename, mode, maxBytes, backupCount, encoding, delay
        )

    def computeRollover(self, currentTime):
        return TimedRotatingFileHandler.computeRollover(self, currentTime)

    def getFilesToDelete(self):
        return TimedRotatingFileHandler.getFilesToDelete(self)

    def doRollover(self):
        return TimedRotatingFileHandler.doRollover(self)

    def shouldRollover(self, record):
        """ Determine if rollover should occur. """
        return TimedRotatingFileHandler.shouldRollover(
            self, record
        ) or RotatingFileHandler.shouldRollover(self, record)


def read_config_file(filename):
    import configobj

    try:
        cfg = configobj.ConfigObj(filename, file_error=True)
    except IOError as e:
        print("cannot read configuration: %s" % e)
        raise
    return cfg


def add_basic_options(parser):
    parser.add_option("--version", action="store_true", help="display the version")
    parser.add_option("--log", dest="log_file", metavar="LOG_FILE", help="log file")
    parser.add_option(
        "--log-dir", dest="log_dir", metavar="LOG_DIR", help="log directory"
    )
    parser.add_option(
        "--debug", action="store_true", help="emit extra diagnostic information"
    )
    parser.add_option(
        "--config", dest="config_file", metavar="CONFIG_FILE", help="configuration file"
    )


def add_db_options(parser):
    """add the standard db options to the command-line parser"""
    parser.add_option("--db-uri", help="database connection parameters as a URI")
    parser.add_option(
        "--db-host", help="host name/address on which database is running"
    )
    parser.add_option(
        "--db-port", type=int, help="port on which database server is listening"
    )
    parser.add_option("--db-name", help="name of the database")
    parser.add_option("--db-user", help="database username")
    parser.add_option("--db-pass", help="database password")


def clean_dir(path):
    if os.path.isdir(path):
        file_list = os.listdir(path)
        for file in file_list:
            if os.path.isfile(path + "/" + file):
                os.remove(path + "/" + file)
        return
    elif os.path.isfile(path):
        os.remove(path)
    return


def create_dir(path):
    if os.path.isdir(path):
        return
    if os.path.isfile(path):
        return
    os.makedirs(path)


def make_pickle(dict, path):
    with open(path, "wb") as file:
        pickle.dump(dict, file)


def read_pickle(path):
    with open(path, "rb") as file:
        return pickle.load(file)


def to_int(x):
    if isinstance(x, str) and x.lower() in ["none", ""]:
        x = None
    return int(x) if x is not None else None


def to_bool(x):
    if x is None:
        return None
    if isinstance(x, str) and x.lower() == "none":
        return None
    try:
        if x.lower() in ["true", "yes", "t"]:
            return True
        elif x.lower() in ["false", "no", "f"]:
            return False
    except AttributeError:
        pass
    try:
        return bool(int(x))
    except (ValueError, TypeError):
        pass
    raise ValueError("Unknown boolean specifier: '%s'." % x)


def int_to_kor_str(value):
    """
    원단위 값 텍스트 표현
    """
    value_txt = str(value)
    if value_txt[0:1] == "-":
        number = int(value_txt[1:])
        sign = "-"
    else:
        number = int(value_txt)
        sign = ""

    # FIXME: 숫자자릿수가 8 이상인 경우 처리
    if len(str(number)) <= 4:
        number_txt = format(number)
    elif len(str(number)) <= 8:
        front_len = len(str(number)) - 4
        number_front = int(str(number)[0:front_len])
        number_back = int(str(number)[front_len:])
        number_txt = format(number_front) + "만"
        if number_back != 0:
            number_txt = number_txt + format(number_back)
    else:
        number_txt = format(number)
    return sign + number_txt


def send_email(text):
    import smtplib
    from email.mime.multipart import MIMEMultipart

    from_addr = "yungyuchoi@thinkpool.com"
    to_addr_list = [
        "mykim5179@thinkpool.com",
        "ilcoffee@thinkpool.com",
        "gr2019065@thinkpool.com",
        "rascu@thinkpool.com",
    ]

    server = smtplib.SMTP("smtp.hiworks.com:587")
    server.starttls()
    server.login(from_addr, "rlawjdgh12!")

    body = MIMEMultipart()
    body["subject"] = text
    body["From"] = from_addr
    body["To"] = ",".join(to_addr_list)
    server.sendmail(from_addr=from_addr, to_addrs=to_addr_list, msg=body.as_string())
    server.quit()


class Killer:
    die_now = False

    def __init__(self):
        signal.signal(signal.SIGTERM, self.exit_now)

    def exit_now(self, signum, frame):
        self.die_now = True