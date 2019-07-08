import os
import logging

dir_path = os.path.dirname(os.path.realpath(__file__))

print dir_path
# LOG_DIR = "tada-gam.log"
#LOG_DIR = os.path.join("tada-gam.log")

LOG_DIR = os.path.join(dir_path, "tada-gam.log")


def set_config(logger, logdir=""):
    if logdir != "":
        handler = logging.FileHandler(logdir)
    else:
        handler = logging.FileHandler(LOG_DIR)
        # handler = logging.FileHandler('tada.log')
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger