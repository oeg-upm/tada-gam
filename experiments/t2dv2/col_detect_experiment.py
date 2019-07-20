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
    processed = all_elected(tables=tables, elect_ports=elect_ports, host_url=host_url)
    while not processed:
        time.sleep(10)
        processed = all_elected(tables=tables, elect_ports=elect_ports, host_url=host_url)

    prec, rec, f1 = get_scores(gold_tables=tables, processed_tables=processed)


def get_scores(gold_tables, processed_tables):
    """
    :param gold_tables:
    :param processed_tables:
    :return:
    """
    correct = 0
    incorrect = 0
    notfound = 0
    for ptable in processed_tables:
        fname = ptable["apple"]
        if ptable["elected"] < 0 and gold_tables[fname] >= 0 :
            print("notfound: "+fname)
            notfound += 1
        elif ptable["elected"] == gold_tables[fname]:
            correct += 1
        else:
            incorrect +=1
            print("incorrect: "+fname)
    prec = correct/(correct+incorrect*1.0)
    rec = correct/(correct+notfound*1.0)
    f1 = prec * rec * 2 / (prec+rec)
    print("correct: %d, incorrect: %d, notfound: %d" % (correct, incorrect, notfound))
    print("precision: %1.3f, recall: %1.3f, f1: %1.3f" % (prec, rec, f1))
    return prec, rec, f1


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
    else:
        print("processed: %d from %d" % (len(processed_tables), len(tables.keys())))
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
    subj_col_dir = os.path.join(DATA_DIR, "subject_column_gold.csv")
    if not os.path.exists(subj_col_dir):
        logger.info("The subject column gold standard file does not exist, so it will be generated")
        classes_dir = os.path.join(DATA_DIR, "classes_GS.csv")
        df = pd.read_csv(classes_dir, header=None)
        fnames_col = df.iloc[:, 0]
        # tables = []
        rows = []
        logger.info("now will fetch the table names from the gold standard")
        for fbase in fnames_col:
            fname = fbase[:-7]+".json"
            fdir = os.path.join(DATA_DIR, "tables", fname)
            subj_col_id = get_subject_column(fdir)
            row = """%s,%d""" % (fbase[:-7], subj_col_id)
            rows.append(row)
        gold_content = "\n".join(rows)
        f = open(subj_col_dir,"w")
        f.write(gold_content)
        f.close()
        logging.info("The subject column gold standard is written successfully")
        # d = {"fname": fbase[:-7]+".tsv", "col_id": subj_col_id}
        # print d
        # tables.append(d)
    # return tables
    df = pd.read_csv(subj_col_dir, header=None)
    tables_d = dict()
    headers = df.columns.values
    for idx, row in df.iterrows():
        fbase = row[headers[0]]
        subj_col_id = int(row[headers[1]])
        tables_d[fbase + ".tsv"] = subj_col_id
    logger.info("subject columns are fetched from the gold standard")
    return tables_d


monitor_spotter()