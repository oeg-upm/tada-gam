from col_detect_experiment import get_tables_and_subject_columns
import os
import subprocess
import captain
import logging
import sys
import requests
import time
import pandas as pd
from app import get_graph, get_labels_from_graph

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

UPLOAD_DIR = "../../local_data/t2dv2"
GOLD_STANDARD_DIR = "data/classes_GS.csv"

PROCESSED_DIR = "processed.txt"


def startup(score=1, combine=1):
    """
    :param score:
    :param combine:
    :param label:
    :return: bool
    """
    captain.parse_args(["up", "--services", "score=%d"%score, "combine=%d"%combine])
    output = subprocess.check_output(["docker-compose", "ps"])
    if len(output.strip().split('\n')) != (score+combine+2):
        logger.error("something seems work with started services")
        logger.error(output)
        return False
    return True


def get_instances(service):
    a = subprocess.check_output(["docker-compose", "ps", service])
    logger.info(a)
    processes = a.split('\n')[2:]
    if len(processes) == 0 or (len(processes) == 1 and processes[0] == ''):
        return []
    instances = []
    print("processes: ")
    print(processes)
    for line in processes:
        if line.strip() != "":
            tokens = filter(None, line.split(' '))
            inst = tokens[0]
            instances.append(inst)
    print("instances: ")
    print(instances)
    return instances


def stop_instance(instances):
    if instances != []:
        a = subprocess.check_output(["docker", "stop"]+instances)
        logger.info(a)


def resume_service(service, num_instances):
    instances = get_instances(service)
    stop_instance(instances)
    success = captain.parse_args(["up", "--services", "%s=%d"%(service, num_instances), "--params", "keep=true"])
    output = subprocess.check_output(["docker-compose", "ps"])
    return success


def run_services(files, cols):
    """
    :param files: list of file dirs
    :param cols: list of subject column indices as strings
    :return:
    """
    args = ["label",
            "--slicesize", "100",
            "--params",
            '%s' % ",".join(cols),
            "--files"] + files
    return captain.parse_args(args)


def get_processed():
    if not os.path.exists(PROCESSED_DIR):
        logger.warning("No previously processed files")
        return []
    f = open(PROCESSED_DIR)
    processed_files = []
    for line in f:
        sline = line.strip()
        if sline == "":
            continue
        processed_files.append(sline)
    f.close()
    return processed_files


def run_labeling():
    ports = captain.get_ports("combine")
    num_combine = len(ports)
    table_d = get_tables_and_subject_columns()
    processed_files = get_processed()
    files = []
    cols = []
    logger.info("processed_files: %d" % len(processed_files))
    for k in table_d.keys():
        fname = k[:-4]+".csv"
        if fname in processed_files:
            logger.info(fname+" is processed already")
            continue
        files.append(os.path.join(UPLOAD_DIR, fname))
        cols.append(str(table_d[k]))

    total_num = len(table_d.keys())
    processed, inprogress = get_apples()
    num_processed = len(processed)
    old_processed = len(processed_files)

    while num_processed+old_processed < total_num:
        time.sleep(10)
        processed, inprogress = get_apples()
        num_processed = len(processed)
        num_inprog = len(inprogress)
        vacant_processing_units = num_combine - num_inprog
        logger.info("currently processed: %d, in progress: %d, vacant_processing_units: %d" % (num_processed, num_inprog, vacant_processing_units))
        if vacant_processing_units > 0:
            to_be_processed = files[:vacant_processing_units]
            col_to_be_proc = cols[:vacant_processing_units]
            remaining = files[vacant_processing_units:]
            col_remaining = cols[vacant_processing_units:]
            run_services(to_be_processed, col_to_be_proc)
            files = remaining
            cols = col_remaining



def workflow():
    f = open("processed.txt", "w")
    f.close()
    if startup(score=12, combine=6):
        run_labeling()



# def monitor_spotter():
#     # spot_ports = captain.get_ports("ssspotter")
#     elect_ports = captain.get_ports("elect")
#     logger.info("elect ports: "+str(elect_ports))
#     # host_ip = captain.get_network_ip()
#     # host_url = "http://"+host_ip
#     host_url = "http://127.0.0.1"
#     # spot_port_id = random.randint(0, len(spot_ports)-1)
#     elect_port_id = random.randint(0, len(elect_ports)-1)
#     tables = get_tables_and_subject_columns()
#     processed = None
#     processed = all_elected(tables=tables, elect_ports=elect_ports, host_url=host_url)
#     while not processed:
#         time.sleep(10)
#         processed = all_elected(tables=tables, elect_ports=elect_ports, host_url=host_url)
#
#     prec, rec, f1 = get_scores(gold_tables=tables, processed_tables=processed)


def get_apples():
    ports_combine = captain.get_ports(service="combine")
    processed = []
    unprocessed = []
    for p in ports_combine:
        url = "http://127.0.0.1:"+p+"/list"
        print("url: "+url)
        response = requests.get(url)
        print(response.json())
        apples = response.json()["apples"]
        for apple in apples:
            if apple["status"] == "Complete":
                processed.append(apple)
            else:
                unprocessed.append(apple)
    return processed, unprocessed


def get_processed_apples_with_ports():
    ports_combine = captain.get_ports(service="combine")
    ports_dict = dict()
    for p in ports_combine:
        url = "http://127.0.0.1:"+p+"/list"
        print("url: "+url)
        response = requests.get(url)
        print(response.json())
        apples = response.json()["apples"]
        processed = []
        for apple in apples:
            if apple["status"] == "Complete":
                processed.append(apple)
        ports_dict[p] = processed
    return ports_dict


def get_gold_standard_dict():
    df = pd.read_csv(GOLD_STANDARD_DIR, header=None)
    d = dict()
    for idx, row in df.iterrows():
        d[row[0][:-7]] = row[2]
    logger.info("get gold standard: %d files" % len(d.keys()))
    return d


def compute_results():
    gold_dict = get_gold_standard_dict()
    print gold_dict
    ports_dict = get_processed_apples_with_ports()
    for port in ports_dict.keys():
        for apple in ports_dict[port]:
            print(apple)
            fbase = apple['table'][:-4]
            logger.info("%s | %s" % (fbase, apple['table']))
            gold_url = gold_dict[fbase]
            alpha = compute_results_of_apple(apple, port, gold_url.strip())
            if alpha >= -2:
                append_results(fbase, alpha)
                logger.info("gethering: %s" % fbase)
            else:
                logger.warning("no graph for %s" % apple['table'])


def append_results(fbase, alpha):
    f = open("label_results.csv", "a")
    f.write("%s,%.7f\n" % (fbase, alpha))
    f.close()


def compute_results_of_apple(apple, port, gold_url):
    fsid = 3
    alphas_fast = [0.1, 0.01, 0.001]
    alphas_rest = [0.45, 0.4, 0.35, 0.3, 0.25, 0.2, 0.05, 0.005, 0.0005, 0.0001, 0.00005, 0.00001]
    # alphas_all = [0.45, 0.4, 0.35, 0.3, 0.25, 0.2, 0.1, 0.05, 0.01, 0.005, 0.001, 0.0005, 0.0001, 0.00005, 0.00001]
    apple_id = apple['id']
    m = apple['m']
    graph_dir = get_graph(port=str(port), apple_id=str(apple_id))
    if graph_dir:
        logger.info("graph_dir is found: "+str(graph_dir))
        for alpha in alphas_fast:
            _, labels = get_labels_from_graph(graph_dir=graph_dir, m=m, alpha=alpha, fsid=fsid)
            if len(labels) == 0:
                return -2
            if labels[0].strip().lower() == gold_url.strip().lower():
                return alpha
        for alpha in alphas_rest:
            _, labels = get_labels_from_graph(graph_dir=graph_dir, m=m, alpha=alpha, fsid=fsid)
            if labels[0].strip().lower() == gold_url.strip().lower():
                return alpha
        return -1
    return -10


if __name__ == "__main__":
    if len(sys.argv) == 2:
        if sys.argv[1] == "resume":
            resume_service("score", 6)
            run_labeling()
        elif sys.argv[1] == "start":
            workflow()
        elif sys.argv[1] == "results":
            #get_gold_standard_dict()
            compute_results()
        else:
            logger.error("expects resume or start parameter")
    else:
        logger.error("expects resume or start parameter")