from col_detect_experiment import get_tables_and_subject_columns
import os
import subprocess
import captain
import logging
import sys
import requests
import time
import pandas as pd
from app import get_graph, get_labels_from_graph, UPLOAD_DIR

NUM_OF_SCORE = 6
NUM_OF_COMBINE = 1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = "../../local_data/t2dv2"
GOLD_STANDARD_DIR = "data/classes_GS.csv"

# PROCESSED_DIR = "processed.txt"
PROCESSED_DIR = "collection.csv"


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
    logger.info("processes: ")
    logger.info(processes)
    for line in processes:
        if line.strip() != "":
            tokens = filter(None, line.split(' '))
            inst = tokens[0]
            instances.append(inst)
    logger.info("instances: ")
    logger.info(instances)
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
    logger.info("\n\n\n\n\n\nLabel to following files: ")
    logger.info(files)
    args = ["label",
            "--slicesize", "10",
            "--params",
            '%s' % ",".join(cols),
            "--files"] + files
    return captain.parse_args(args)


# def get_processed():
#     if not os.path.exists(PROCESSED_DIR):
#         logger.warning("No previously processed files")
#         return []
#     f = open(PROCESSED_DIR)
#     processed_files = []
#     for line in f:
#         sline = line.strip()
#         if sline == "":
#             continue
#         processed_files.append(sline)
#     f.close()
#     return processed_files


def get_processed():
    processed = []
    if os.path.exists(PROCESSED_DIR):
        with open(PROCESSED_DIR) as f:
            for line in f.read().split('\n'):
                if ',' in line:
                    fname, m, gname = line.split(',')
                    processed.append(fname)
        logger.info("processed files: %d" % len(processed))
    else:
        logger.warning("No previously processed files: %s" % PROCESSED_DIR)

    return processed


def run_labeling():
    ports = captain.get_ports("combine")
    num_combine = len(ports)
    score_ports = captain.get_ports("score")
    num_score = len(score_ports)
    table_d = get_tables_and_subject_columns()
    processed_files = get_processed()
    files = []
    cols = []
    logger.info("processed_files: %d" % len(processed_files))
    for k in table_d.keys():
        fname = k[:-4]+".csv"
        #fname = k
        if fname in processed_files:
            logger.info(fname+" is processed already")
            continue
        files.append(os.path.join(DATA_DIR, fname))
        cols.append(str(table_d[k]))

    total_num = len(table_d.keys())
    processed, inprogress = get_apples()
    # processed, inprogress = [], []
    num_processed = len(processed)
    old_processed = len(processed_files)

    logger.info("processed: %d, old processed: %d, total: %d" % (
    num_processed, old_processed, total_num))

    while num_processed+old_processed < total_num:
        processed, inprogress = get_apples()
        num_processed = len(processed)
        num_inprog = len(inprogress)
        vacant_processing_units = 1 - num_inprog  #(num_combine - num_inprog)
        logger.info("currently processed: %d, in progress: %d, vacant_processing_units: %d" % (num_processed, num_inprog, vacant_processing_units))
        if vacant_processing_units > 0:
            logger.info("Available vacant: vacant_processing_units: %d\n\n\n\n" % num_processed)
            a = raw_input("Enter any key to continue: ")
            to_be_processed = files[:vacant_processing_units]
            col_to_be_proc = cols[:vacant_processing_units]
            remaining = files[vacant_processing_units:]
            col_remaining = cols[vacant_processing_units:]
            run_services(to_be_processed, col_to_be_proc)
            files = remaining
            cols = col_remaining

        time.sleep(60)


    logger.info("The END .. processed: %d, old processed: %d, total: %d" % (
    num_processed, old_processed, total_num))


def workflow():
    # f = open("processed.txt", "w")
    # f.close()
    if startup(score=NUM_OF_SCORE, combine=NUM_OF_COMBINE):
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
        # print("url: "+url)
        response = requests.get(url)
        # print(response.json())
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


# def compute_results():
#     f = open("label_results.csv", "w")
#     f.close()
#     gold_dict = get_gold_standard_dict()
#     print gold_dict
#     ports_dict = get_processed_apples_with_ports()
#     for port in ports_dict.keys():
#         for apple in ports_dict[port]:
#             print(apple)
#             fbase = apple['table'][:-4]
#             logger.info("%s | %s" % (fbase, apple['table']))
#             gold_url = gold_dict[fbase]
#             alpha = compute_results_of_apple(apple, port, gold_url.strip())
#             if alpha >= -2:
#                 append_results(fbase, alpha)
#                 logger.info("gethering: %s" % fbase)
#             else:
#                 logger.warning("no graph for %s" % apple['table'])

def compute_results():
    f = open("label_results.csv", "w")
    f.close()
    gold_dict = get_gold_standard_dict()
    print gold_dict
    content = ""
    with open(PROCESSED_DIR) as f:
        content = f.read()

    for line in content.split("\n"):
        if ',' in line:
            fname, m, gname = line.split(',')
            m = int(m)
            fbase = fname[:-4]
            logger.info("%s | %s" % (fbase, fname))
            gold_url = gold_dict[fbase]
            alpha = compute_results_of_file(gname=gname, m=m, gold_url=gold_url.strip())
            if alpha >= -2:
                append_results(fbase, alpha)
                logger.info("gethering: %s" % fbase)
            else:
                logger.warning("no graph for %s" % fname)


def append_results(fbase, alpha):
    f = open("label_results.csv", "a")
    f.write("%s,%.7f\n" % (fbase, alpha))
    f.close()


def compute_results_scores():
    f = open("label_results.csv")
    notfound = 0
    correct = 0
    incorrect = 0
    for line in f:
        row = line.split(',')
        if len(row) == 2:
            alpha = float(row[1])
            if alpha == -1:
                incorrect += 1
            elif alpha >= 0:
                correct += 1
            else:
                notfound += 1
    prec = correct/(correct+incorrect*1.0)
    rec = correct/(correct+notfound*1.0)
    f1 = prec * rec * 2 / (prec+rec)
    print("correct: %d, incorrect: %d, notfound: %d" % (correct, incorrect, notfound))
    print("precision: %1.3f, recall: %1.3f, f1: %1.3f" % (prec, rec, f1))

#
# def compute_results_of_apple(apple, port, gold_url):
#     fsid = 3
#     alphas_fast = [0.1, 0.01, 0.001]
#     alphas_rest = [0.45, 0.4, 0.35, 0.3, 0.25, 0.2, 0.05, 0.005, 0.0005, 0.0001, 0.00005, 0.00001]
#     # alphas_all = [0.45, 0.4, 0.35, 0.3, 0.25, 0.2, 0.1, 0.05, 0.01, 0.005, 0.001, 0.0005, 0.0001, 0.00005, 0.00001]
#     apple_id = apple['id']
#     m = apple['m']
#     graph_dir = get_graph(port=str(port), apple_id=str(apple_id))
#     if graph_dir:
#         logger.info("graph_dir is found: "+str(graph_dir))
#         for alpha in alphas_fast:
#             _, labels = get_labels_from_graph(graph_dir=graph_dir, m=m, alpha=alpha, fsid=fsid)
#             if len(labels) == 0:
#                 return -2
#             if labels[0].strip().lower() == gold_url.strip().lower():
#                 return alpha
#         for alpha in alphas_rest:
#             _, labels = get_labels_from_graph(graph_dir=graph_dir, m=m, alpha=alpha, fsid=fsid)
#             if labels[0].strip().lower() == gold_url.strip().lower():
#                 return alpha
#         return -1
#     return -10


def compute_results_of_file(gname, m, gold_url):
    fsid = 3
    alphas_fast = [0.1, 0.01, 0.001]
    alphas_rest = [0.45, 0.4, 0.35, 0.3, 0.25, 0.2, 0.02, 0.05, 0.005, 0.0005, 0.0001, 0.00005, 0.00001]
    # alphas_all = [0.45, 0.4, 0.35, 0.3, 0.25, 0.2, 0.1, 0.05, 0.01, 0.005, 0.001, 0.0005, 0.0001, 0.00005, 0.00001]
    graph_dir = os.path.join(UPLOAD_DIR, gname)
    logger.info("gname: %s" % gname)
    logger.info("UPLOAD_DIR: %s" % UPLOAD_DIR)
    logger.info("graph dir: %s" % graph_dir)
    if graph_dir:
        logger.info("graph_dir is found: "+str(graph_dir))
        for alpha in alphas_fast:
            _, labels = get_labels_from_graph(graph_dir=graph_dir, m=m, alpha=alpha, fsid=fsid)
            if len(labels) == 0:
                return -2
            if labels[0].strip().lower() == gold_url.strip().lower():
                return alpha
            elif gname=="4__bfjkevshrvwi_graph.json":
                logger.info("\n\n\n\n(%f)<%s> != <%s>" % (alpha, labels[0].strip().lower(), gold_url.strip().lower()))
                logger.info("DEBUG graph_dir: <%s>, alpha: <%f>, m: <%d>, fsid: <%d>" % (graph_dir, alpha, m, fsid))
        for alpha in alphas_rest:
            _, labels = get_labels_from_graph(graph_dir=graph_dir, m=m, alpha=alpha, fsid=fsid)
            if labels[0].strip().lower() == gold_url.strip().lower():
                return alpha
        return -1
    return -10


def custom_compute_results_for_file(fname, alpha):
    gold_dict = get_gold_standard_dict()
    print gold_dict
    content = ""
    with open(PROCESSED_DIR) as f:
        content = f.read()

    is_found = False

    for line in content.split("\n"):
        if ',' in line:
            fname2, m, gname = line.split(',')
            m = int(m)
            if fname2 != fname:
                continue
            else:
                fbase = fname[:-4]
                # logger.info("line: %s" % line)
                # logger.info("gname one: %s" % gname)
                logger.info("%s | %s" % (fbase, fname))
                gold_url = gold_dict[fbase]
                is_found = True
                break

    if not is_found:
        return -20

    fsid = 3
    graph_dir = os.path.join(UPLOAD_DIR, gname)
    logger.info("gname: %s" % gname)
    logger.info("UPLOAD_DIR: %s" % UPLOAD_DIR)
    logger.info("graph dir: %s" % graph_dir)
    if graph_dir:
        logger.info("graph_dir is found: "+str(graph_dir))
        _, labels = get_labels_from_graph(graph_dir=graph_dir, m=m, alpha=alpha, fsid=fsid)
        if len(labels) == 0:
            return -2
        if labels[0].strip().lower() == gold_url.strip().lower():
            # logger.info("<%s> == <%s>" % (labels[0].strip().lower(), gold_url.strip().lower()))
            return alpha
        logger.info("<%s> != <%s>" % (labels[0], gold_url))
        logger.info("<%s> != <%s>" % (labels[0].strip().lower(), gold_url.strip().lower()))
        logger.info("2nd %s" % labels[1])
        logger.info("3rd %s" % labels[2])
        logger.info("m: %d" % m)
        logger.info("alpha: %f" % alpha)
        return -1
    return -10


def graph_collector():
    collection_fname = PROCESSED_DIR
    processed = get_processed()
    while len(processed) < 237:
        _, inprogress = get_apples()
        logger.info("processed: %d" % len(processed))
        logger.info("In progress: %d" % len(inprogress))

        ports_dict = get_processed_apples_with_ports()
        for port in ports_dict.keys():
            for apple in ports_dict[port]:
                if apple['table'] not in processed:
                    logger.info("collecting results for: %s" % apple['table'])
                    with open(collection_fname, "a") as f:
                        gdir = get_graph(port, str(apple['id']))
                        gname = gdir.split(os.sep)[-1]
                        line = "%s,%d,%s\n" % (apple['table'], apple['m'], gname)
                        f.write(line)
                    processed.append(apple['table'])
        time.sleep(60)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        # if sys.argv[1] == "resume":
        #     resume_service("score", NUM_OF_SCORE)
        #     run_labeling()
        if sys.argv[1] == "start":
            workflow()
        elif sys.argv[1] == "results":  # get results from combine instances
            compute_results()
        # show scores for the gotten results (this is typically called after compute_results)
        elif sys.argv[1] == "show":
            compute_results_scores()
        elif sys.argv[1] == "collect":
            graph_collector()
        else:
            logger.error("expects resume or start parameter")
    elif len(sys.argv) == 4:
        if sys.argv[1] == "single":  # compute the results for a single
            a = custom_compute_results_for_file(fname=sys.argv[2], alpha=float(sys.argv[3]))
            print("result: %s" % str(a))
    else:
        logger.error("expects resume or start parameter")
