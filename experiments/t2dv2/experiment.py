import sys
reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append('../../')

import chardet
import time
import os
import random
import pandas as pd
import json
import requests
import captain
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DATA_DIR = "data"
#DATA_DIR = "experiments/t2dv2/data"
UPLOAD_DIR = "../local_data/t2dv2"


def monitor_spotter():
    # spot_ports = captain.get_ports("ssspotter")
    elect_ports = captain.get_ports("elect")
    logger.info("elect ports: "+str(elect_ports))
    # host_ip = captain.get_network_ip()
    # host_url = "http://"+host_ip
    host_url = "http://127.0.0.1"
    # spot_port_id = random.randint(0, len(spot_ports)-1)
    elect_port_id = random.randint(0, len(elect_ports)-1)
    tables = get_tables_and_subject_columns()
    processed = None
    while not processed:
        time.sleep(10)
        processed = all_elected(tables=tables, elect_ports=elect_ports, host_url=host_url)

    prec = get_scores(gold_tables=tables, processed_tables=processed)


def get_scores(gold_tables, processed_tables):
    """
    :param gold_tables:
    :param processed_tables:
    :return:
    """
    correct = 0
    incorrect = 0
    for ptable in processed_tables:
        fname = ptable["apple"]
        if ptable["elected"] == gold_tables[fname]:
            correct += 1
        else:
            incorrect +=1
            print("incorrect: "+fname)
    prec = correct/(correct+incorrect*1.0)
    print("correct: %d, incorrect: %d, precision: %1.3f" % (correct, incorrect, prec))
    return prec


def all_elected(tables, elect_ports, host_url):
    processed_tables = []
    for elect_port in elect_ports:
        url = host_url+":"+str(elect_port)+"/status"
        response = requests.get(url)
        j = response.json()
        for table in j["apples"]:
            if table["status"] != "Complete":
                print("processed until now: %d" % len(processed_tables))
                return None
            processed_tables.append(table)
    if len(processed_tables) == len(tables.keys()):
        return processed_tables
    return None


def get_subject_column(fdir):
    """
    :param fdir:
    :return:
    """
    f = open(fdir)
    s = f.read()
    detected_encoding = chardet.detect(s)['encoding']
    decoded_s = s.decode(detected_encoding)
    j = json.loads(decoded_s)
    if j["hasKeyColumn"]:
        return j["keyColumnIndex"]
    else:
        return -1


def get_tables_and_subject_columns():
    classes_dir = os.path.join(DATA_DIR, "classes_GS.csv")
    df = pd.read_csv(classes_dir)
    fnames_col = df.iloc[:, 0]
    # tables = []
    tables_d = dict()
    logger.info("now will fetch the table names from the gold standard")
    for fbase in fnames_col:
        fname = fbase[:-7]+".json"
        fdir = os.path.join(DATA_DIR, "tables", fname)
        subj_col_id = get_subject_column(fdir)
        tables_d[fbase[:-7]+".tsv"] = subj_col_id
        # d = {"fname": fbase[:-7]+".tsv", "col_id": subj_col_id}
        # print d
        # tables.append(d)
    # return tables
    logger.info("subject columns are fetched from the gold standard")
    return tables_d


def test():
    logger.debug("Hello")

# test()

monitor_spotter()