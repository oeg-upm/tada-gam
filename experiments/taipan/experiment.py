import subprocess
import logging
import random
from datetime import datetime
import commons
import os
import time
import glob
import requests
import sys
reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append('../../')
import captain
import experiments.commons as expcommons


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = "data"


def startup(spot=1, elect=1):
    captain.parse_args(["up", "--services", "ssspotter=%d"%spot, "elect=%d"%elect])
    output = subprocess.check_output(["docker-compose", "ps"])
    if len(output.strip().split('\n')) != (spot+elect+2):
        logger.error("something seems work with started services")
        logger.error(output)
        return False
    return True


def run_spot_and_elect(spot_technique, elect_technique):
    """
    :param spot_technique:
    :param elect_technique:
    :return:
    """
    files_pattern = os.path.join(DATA_DIR, "t2d", "*.csv")
    files_dir = glob.glob(files_pattern)
    args = ["spot",
            "--params",
            'spot_technique=%s,elect_technique=%s' % (spot_technique, elect_technique),
            "--files"] + files_dir
    return captain.parse_args(args)


def pairs_to_dict(pairs):
    d = dict()
    for p in pairs:
        csv_fname = p['fname']
        tsv_fname = csv_fname[:-4]+".tsv"
        d[tsv_fname] = p['col_id']
    return d


def monitor_spotter():
    """
    :return:
    """
    elect_ports = captain.get_ports("elect")
    logger.info("elect ports: "+str(elect_ports))
    host_url = "http://127.0.0.1"
    elect_port_id = random.randint(0, len(elect_ports)-1)
    tables_gold = commons.get_t2d_gold()
    tables_gold_d = pairs_to_dict(tables_gold)
    processed = expcommons.get_elected_tables(gold_tables=tables_gold_d, elect_ports=elect_ports, host_url=host_url)
    while not processed:
        time.sleep(10)
        processed = expcommons.get_elected_tables(gold_tables=tables_gold_d, elect_ports=elect_ports, host_url=host_url)

    prec, rec, f1 = expcommons.get_scores(gold_tables=tables_gold_d, processed_tables=processed)
    return prec, rec, f1


def workflow():
    scores_path = os.path.join(DATA_DIR, "scores.csv")
    f = open(scores_path, "w")
    f.close()
    SPOT_TECHNIQUES = ["left_most", "left_most_non-numeric"]
    ELECT_TECHNIQUES = ["majority", "found-majority"]
    for spot_tech in SPOT_TECHNIQUES:
        for elect_tech in ELECT_TECHNIQUES:
            start_time = datetime.now()
            startup(spot=6, elect=3)
            if run_spot_and_elect(spot_technique=spot_tech, elect_technique=elect_tech):
                prec, rec, f1 = monitor_spotter()
                end_time = datetime.now()
                f = open(scores_path, "a")
                f.write("%s,%s,%.2f,%.2f,%.2f,%.2f minutes\n" % (spot_tech, elect_tech, prec, rec, f1,
                                                                 (end_time-start_time).seconds/60.0))
                f.close()
            else:
                logger.error("Error in run spot and elect")
                return


if __name__ == "__main__":
    workflow()
