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
import argparse
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DATA_DIR = "data"
#DATA_DIR = "experiments/t2dv2/data"
UPLOAD_DIR = "../../local_data/t2dv2"


def monitor_spotter():
    elect_ports = captain.get_ports("elect")
    logger.info("elect ports: "+str(elect_ports))
    # host_ip = captain.get_network_ip()
    # host_url = "http://"+host_ip
    host_url = "http://127.0.0.1"
    # spot_port_id = random.randint(0, len(spot_ports)-1)
    elect_port_id = random.randint(0, len(elect_ports)-1)
    tables = get_tables_and_subject_columns()
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
    logger.info("correct: %d, incorrect: %d, notfound: %d" % (correct, incorrect, notfound))
    logger.info("precision: %1.3f, recall: %1.3f, f1: %1.3f" % (prec, rec, f1))
    return prec, rec, f1


def all_elected(tables, elect_ports, host_url):
    processed_tables = []
    not_complete = False
    for elect_port in elect_ports:
        url = host_url+":"+str(elect_port)+"/status"
        response = requests.get(url)
        j = response.json()
        for table in j["apples"]:
            if table["status"] != "Complete":
                # logger.info("[elect:%s]processed until now: %d" % (str(elect_port), len(processed_tables)))
                not_complete = True
                # return None
            processed_tables.append(table)
        logger.info("[elect:%s]processed until now: %d" % (str(elect_port), len(processed_tables)))

    if not_complete:
        return None
    if len(processed_tables) == len(tables.keys()):
        return processed_tables
    else:
        logger.info("processed: %d from %d" % (len(processed_tables), len(tables.keys())))
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


def run_detection(spot_technique, elect_technique, sample):
    """
    :param spot_technique:
    :param elect_technique:
    :param sample:
    :return:
    """
    tables_d = get_tables_and_subject_columns()
    fdirs = []
    for t in tables_d.keys():
        tname = t[:-4] + ".csv"
        tdir = os.path.join(os.path.abspath(UPLOAD_DIR), tname)
        # tdir = os.path.join(UPLOAD_DIR, tname)
        fdirs.append(tdir)

    run_services(files=fdirs, elect_technique=elect_technique, spot_technique=spot_technique, sample=sample)


def run_services(files, elect_technique, spot_technique, sample):
    """
    :param files: list of file dirs
    :param sample: the sample method
    :param elect_technique: the elect technique
    :param spot_technique: the spot technique
    :return:
    """
    logger.info("\n\n\n\n\n\nSpot to following files: ")
    logger.info(files)
    args = ["spot",
            "--sample", sample,
            "--slicesize", "10",
            "--params",
            "elect_technique=%s,spot_technique=%s" % (elect_technique, spot_technique),
            "--files"] + files
    return captain.parse_args(args)


def startup(spot=1, elect=1):
    """
    :param spot: number of spot instances
    :param elect: number of elect instances
    :return: bool
    """
    captain.parse_args(["up", "--services", "ssspotter=%d" % spot, "elect=%d" % elect])
    output = subprocess.check_output(["docker-compose", "ps"])
    if len(output.strip().split('\n')) != (spot + elect + 2):
        logger.error("something seems wrong with the started services, make sure to expand the terminal window")
        logger.error(output)
        return False
    return True


def parse_args(args=None):
    global logger
    actions_desc = """
        "start":      To start the services
        "detect":     To run the detection experiment
        "monitor":    To monitor the progress
    """
    parser = argparse.ArgumentParser(description='To detect the subject column')
    parser.add_argument('action', help=''+actions_desc, choices=['start', 'detect', 'monitor'])
    parser.add_argument('--spot_technique', choices=["left_most", "left_most_non-numeric", "most_distinct"],
                        help="Enter the spot technique")
    parser.add_argument('--elect_technique', choices=["majority", "found-majority"],
                        help="Enter the elect technique")
    parser.add_argument('--sample', help="The sampling method",
                        choices=['all', '10'])
    parser.add_argument('--log', help="The name of the log file")
    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)

    if args.log:
        handler = logging.FileHandler(args.log)
        logger.addHandler(handler)
        formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
    if args.action == "start":
        startup(spot=6, elect=3)
    elif args.action == "detect":
        if not args.spot_technique :
            logger.error("spot_technique is required")
            parser.print_help()
            return
        if args.elect_technique is None:
            logger.error("elect_technique is required")
            parser.print_help()
            return
        if args.sample is None:
            logger.error("sample is required")
            parser.print_help()
            return
        run_detection(elect_technique=args.elect_technique, spot_technique=args.spot_technique, sample=args.sample)
        monitor_spotter()
    elif args.action == "monitor":
        monitor_spotter()
    else:
        parser.print_help()


if __name__ == "__main__":
    parse_args()

