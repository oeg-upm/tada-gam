import argparse
import subprocess
import random
import os
import requests
import pandas as pd
import io
import logging
from time import sleep


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('captain')


# This is the default network created by docker
NETWORK = "tada-gam_default"
SLEEP_TIME = 60  # seconds


def get_network_ip():
    """
    Get the ip of the network
    :return:
    """
    comm = """ docker network inspect tada-gam_default | grep "Gateway" """
    a = subprocess.Popen(comm, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()[0]
    ip = a.split(":")[1].replace('"', '').strip()
    return ip


def get_all_ports():
    a = subprocess.check_output(["docker-compose", "ps"])
    logger.info(a)
    processes = a.split('\n')[2:]
    ports = []
    for line in processes:
        if line.strip() != "":
            port = get_port(line)
            if port is not None:
                ports.append(int(port))
    return ports


def get_ports(service):
    a = subprocess.check_output(["docker-compose", "ps", service])
    logger.info(a)
    combine_processes = a.split('\n')[2:]
    ports = []
    for line in combine_processes:
        if line.strip() != "":
            port = get_port(line)
            if port is not None:
                ports.append(port)
    return sorted(ports)


def get_port(line):
    tokens = filter(None, line.split(' '))
    # print("tokens: ")
    # print(tokens)
    status = tokens[-2]
    if status.strip() == "Up":
        port = tokens[-1].split('->')[0].split(':')[1]
    else:
        port = None
    return port


def detect_entity_cols(file):
    """
    For now, we assume that the entity column is the first one
    :param file:
    :return: a list of indices of entity columns
    """
    return [0]


def label_column(file_dir, col, port, slice_size, score_ports):
    """
    :param file_dir: the directory of the input file
    :param col: col index
    :param slice_size: integer of the slice size
    :param port: port
    :param score_ports: a list of score ports
    :return:
    """
    logger.info("\n\ncombine> file: %s, col: %d, port: %s" % (file_dir, col, port))
    COMBINE_HOST = "http://"+get_network_ip()
    num_ports = len(score_ports)
    # i = random.randint(0, num_ports-1)
    df = pd.read_csv(file_dir)
    dfcol = df.iloc[:, col]
    # dfcol = df.iloc[:, 0]
    fname = file_dir.split(os.sep)[-1]
    total_num_slices = dfcol.shape[0]/slice_size
    if dfcol.shape[0]%slice_size != 0:
        total_num_slices += 1
    score_port_idx = random.randint(0, num_ports-1)
    for slice_idx in range(total_num_slices):
        label_a_slice(slice_idx=slice_idx, total_num_slices=total_num_slices, score_ports=score_ports,
                      file_dir=file_dir, col=col, combine_port=port, combine_host=COMBINE_HOST,
                      slice_size=slice_size, fname=fname, dfcol=dfcol)

        # score_port = score_ports[(score_port_idx+slice_idx)%num_ports]
        # logger.info("score> file: %s, col: %d, slice: %d, score_port: %s, combine_port: %s" % (file_dir, col, slice_idx,
        #                                                                                  score_port, port))
        # slice_from = slice_idx*slice_size
        # slice_to = min(slice_from + slice_size, dfcol.shape[0]-1)  # to cover the cases where the last slice is not full
        # # print("fname: <%s> slicefrom: %d, to: %d\n" % (fname, slice_from, slice_to)),
        # # print(dfcol)
        # files = {'file_slice': (fname, "\t".join(dfcol[slice_from:slice_to].astype(str).values.tolist()))}
        # values = {'table': fname, 'column': col, 'slice': slice_idx, 'total': total_num_slices,
        #           'addr': COMBINE_HOST+":"+str(port)}
        # score_url = "http://127.0.0.1:"+str(score_port)+"/score"
        # r = requests.post(score_url, files=files, data=values)
        # if r.status_code != 200:
        #     logger.error("error: "+str(r.content))
        # # i = i + 1
        # # i = i % num_ports


def label_a_slice(slice_idx, total_num_slices, score_ports, file_dir, col, combine_port, combine_host,
                  slice_size, fname, dfcol):
    """
    :param slice_idx: The index of a slice
    :param total_num_slices:  The total number of slices
    :param score_ports: the list of score ports
    :param file_dir: the directory of the file (passed here just for logging)
    :param col: the index of the column
    :param combine_port: the target combine port
    :param combine_host: the http url of the combine (without the port)
    :param slice_size: the size of the slize
    :param fname: the name of the file (it will be used as an identifier for validating the labeing and combining slices)
    :param dfcol: the dataframe of the column (the whole one)
    :return:
    """

    num_ports = len(score_ports)
    score_port_idx_start = random.randint(0, num_ports - 1)
    while True:
        for i in range(num_ports):
            score_port_idx = (score_port_idx_start + i) % num_ports

            score_port = score_ports[score_port_idx]
            num_complete, num_inprog = get_score_load(score_port)
            if num_inprog == 0:  # the score is not working on something else

                logger.info("score> file: %s, col: %d, slice: %d, score_port: %s, combine_port: %s" % (file_dir, col,
                                                                                                       slice_idx,
                                                                                                       score_port,
                                                                                                       combine_port))
                slice_from = slice_idx * slice_size
                slice_to = min(slice_from + slice_size,
                               dfcol.shape[0] - 1)  # to cover the cases where the last slice is not full
                files = {'file_slice': (fname, "\t".join(dfcol[slice_from:slice_to].astype(str).values.tolist()))}
                values = {'table': fname, 'column': col, 'slice': slice_idx, 'total': total_num_slices,
                          'addr': combine_host + ":" + str(combine_port)}
                score_url = "http://127.0.0.1:" + str(score_port) + "/score"
                r = requests.post(score_url, files=files, data=values)
                if r.status_code != 200:
                    logger.error("error: " + str(r.content))
                return
        sleep(SLEEP_TIME)


def get_score_load(score_port):
    """
    Get the number of inprogress and completed runs
    :param score_port: str or num
    :return: int, int
    """
    score_url = "http://127.0.0.1:" + str(score_port) + "/list"
    response = requests.get(score_url)
    j = response.json()
    complete = 0
    inprogress = 0
    for b in j["bites"]:
        if b["status"] == "complete":
            complete += 1
        else:
            inprogress += 1
    return complete, inprogress


def up_services(services, keep):
    """
    :param services: list of strings
    :param keep: whether to keep the running service or not
    :return:
    """
    if not keep:
        # Shutdown running instances
        comm = "docker-compose down"
        subprocess.call(comm, shell=True)

    # # Rebuild images
    # comm = "docker-compose build"
    # subprocess.call(comm, shell=True)

    # Running services
    port = 5100
    occ_ports = get_all_ports()
    if occ_ports != []:
        port = max(occ_ports)+1
    for service_instance in services:
        if '=' in service_instance:
            service, instances = service_instance.split('=')
        else:
            instances = 1
            service = service_instance

        for i in range(int(instances)):
            in_port = port
            if run_service(service, port, in_port):
                port += 1
            else:
                logger.error("Error in running service: %s" % (service))
                return False

    comm = "docker-compose ps"
    subprocess.call(comm, shell=True)
    return True


def combine_status():
    ports_combine = get_ports(service="combine")
    apples = []
    for p in ports_combine:
        url = "http://127.0.0.1:"+p+"/status"
        print("url: "+url)
        response = requests.get(url)
        apples += response.json()["apples"]

    print("%30s     %20s" % ("Apple", "status"))
    print("%30s     %20s" % ("---------", "---------"))

    for a in apples:
        print("%30s     %20s" % (a["apple"], a["status"]))

    print("------------------------------------------")
    print("total: %d\n" % len(apples))


def run_service(service, out_port, in_port):
    """
    :param service: The name of the service
    :param out_port: The exposed (published) port (accessible from host)
    :param in_port: The local port where the application is running on
    :return:
    """
    comm = "docker-compose run -d -e port=%d -p %d:%d %s" % (in_port, out_port, in_port, service)
    print("comm: "+comm)
    exit_code = subprocess.call(comm, shell=True)
    return exit_code == 0


def label_files(files, slice_size, cols):
    """
    :param files: the directory of the files
    :param slice_size:
    :params cols: the list of subject columns (ids)
    :return:
    """
    ports_combine = get_ports(service="combine")
    ports_score = get_ports(service="score")
    num_ports = len(ports_combine)
    i = random.randint(0, num_ports-1)

    for idx, f in enumerate(files):
        if cols == []:
            entity_cols = detect_entity_cols(f)
        else:
            entity_cols = [cols[idx]]
        for c in entity_cols:
            logger.info("label_column in file (%d): %s" %(idx, f))
            label_column(file_dir=f, col=c, slice_size=slice_size, port=ports_combine[i], score_ports=ports_score)
            i = i+1
            i = i % num_ports
        # ff = open("processed.txt", "a")
        # ff.write(f.split(os.sep)[-1]+"\n")
        # ff.close()


def spot_in_a_file(file_dir, slice_size, spotters_ports, elector_port, elect_technique, spot_technique):
    """
    :param file_dir:
    :param slice_size:
    :param spotters_ports:
    :param elector_port:
    :param spot_technique:
    :param elect_technique:
    :return:
    """
    logger.info("\n\nspot> file: %s, elector port: %s" % (file_dir, str(elector_port)))
    elect_host = "http://"+get_network_ip()
    num_ports = len(spotters_ports)
    i = random.randint(0, num_ports-1)
    df = pd.read_csv(file_dir)
    fname = file_dir.split(os.sep)[-1]
    if fname[-4:].lower() == ".csv":
        fname = fname[:-4] + ".tsv"
    else:
        fname = fname + ".tsv"

    total_num_slices = df.shape[0]/slice_size
    if df.shape[0]%slice_size != 0:
        total_num_slices += 1
    print("shape: %d" % df.shape[0])
    print("slice size: %d" % slice_size)
    print("dev: %d" % (df.shape[0]/slice_size))
    print("rem: %d" % (df.shape[0]%slice_size))
    port_idx = random.randint(0, num_ports-1)
    for slice_idx in range(total_num_slices):
        port = spotters_ports[(port_idx+slice_idx)%num_ports]
        logger.info("spot> file: %s, slice: %d, spot port: %s, elect port: %s" % (file_dir, slice_idx,
                                                                                         port, elector_port))
        slice_from = slice_idx*slice_size
        slice_to = min(slice_from + slice_size, df.shape[0])  # to cover the cases where the last slice is not full
        rows = []
        print("slice from: %d" % slice_from)
        print("slice to: %d" % slice_to)
        for row_items in df[slice_from:slice_to].values.tolist():
            # print(type(row_items))
            # print(row_items)
            # if len(row_items) == 1:
            #     row = row_items
            # else:
            row_items_s = [str(s) for s in row_items]
            row = "\t".join(row_items_s)
            rows.append(row)
        if rows == []:
            print("\n\n\n ZEEROOOO")
            return
        file_content = "\n".join(rows)
        files = {'table': (fname, file_content)}
        values = {'technique': spot_technique, 'slice': slice_idx, 'total': total_num_slices,
                  'callback': elect_host+":"+str(elector_port)+"/add?technique="+elect_technique}
        spotter_url = "http://127.0.0.1:"+str(port)+"/spot"
        r = requests.post(spotter_url, files=files, data=values)
        if r.status_code != 200:
            logger.error("error: "+str(r.content))
        i = i + 1
        i = i % num_ports


def spot_in_files(files, slice_size, spotter, elect_technique, spot_technique):
    """
    :param files: list of files
    :param slice_size: the size of each slice (rows and cols)
    :param spotter:
    :return:
    """
    spotters_ports = get_ports(spotter)
    elector_ports = get_ports("elect")
    i = random.randint(0, len(elector_ports)-1)
    for f in files:
        spot_in_a_file(file_dir=f, slice_size=slice_size, spotters_ports=spotters_ports, elector_port=elector_ports[i],
                       elect_technique=elect_technique, spot_technique=spot_technique)
        i = i+1
        i = i % len(elector_ports)


def parse_args(args=None):
    parser = argparse.ArgumentParser(description='Captain to look after the processes')
    parser.add_argument('action', help='What action you like to perform', choices=['label', 'spot', 'ports', 'up',
                                                                                   'status'])
    parser.add_argument('--files', nargs='+', help="The set of file to be labeled")
    # parser.add_argument('--cols', nargs='+', help="The indices of the columns (starting from 0)")
    parser.add_argument('--slicesize', help="The max number of elements in a slice", type=int, default=10)
    # parser.add_argument('--instances', nargs='+', help="The numbers of instances (as a list)")
    parser.add_argument('--services', nargs='+', help="The names of the services")
    # parser.add_argument('--dir', help="The directory of the input files to be labeled")
    help_txt = """Extra parameters. It should be a string of key value pairs separated by ',' for subject column 
    detection and a list of subject column ids for the semantic labeling case.
    """
    parser.add_argument('--params', help=help_txt)
    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)
    action = args.action
    if action == "ports":
        ports_combine = get_ports(service="combine")
        ports_score = get_ports(service="score")
        print("combine: "+str(ports_combine))
        print("score: "+str(ports_score))
        return True
    elif action == "label":
        if args.files:
            cols = []
            if args.params:
                try:
                    cols = [int(c) for c in args.params.split(",")]
                    if len(cols) != len(args.files):
                        logger.error("Number of subject column ids should match the number of files")
                        return False
                except Exception as e:
                    logger.error("params for label should be a comma-separated list of natural numbers (col ids)")
                    logger.error(str(e))
                    return False
            logger.info("columns ids: %s" % str(cols))
            label_files(files=args.files, slice_size=args.slicesize, cols=cols)
            return True
        else:
            parser.print_help()
            return False
    elif action == "spot":
        SPOT_TECHNIQUES = ["left_most", "left_most_non-numeric"]
        ELECT_TECHNIQUES = ["majority", "found-majority"]
        if args.files:
            spotter = "ssspotter"
            elect_technique = ""
            spot_technique = ""
            if args.params:
                for p in args.params.split(","):
                    k, v = p.split("=")
                    if k == "elect_technique":
                        elect_technique = v
                    elif k == "spot_technique":
                        spot_technique = v
            if elect_technique=="":
                logger.error("missing elect_technique in params")
            if spot_technique=="":
                logger.error("missing spot_technique in params")
            if elect_technique not in ELECT_TECHNIQUES:
                logger.error("elect_technique should have one of these values: %s" % (str(ELECT_TECHNIQUES)))
                return False
            if spot_technique not in SPOT_TECHNIQUES:
                logger.error("spot_technique should have one of these values: %s" % (str(SPOT_TECHNIQUES)))
                return False
            spot_in_files(files=args.files, slice_size=args.slicesize, spotter=spotter,
                          elect_technique=elect_technique, spot_technique=spot_technique)
            return True
        else:
            parser.print_help()
            return False
    elif action == "up":
        if args.services:
            keep = False
            if args.params:
                if len(args.params.split('=')) == 2:
                    ke, va = args.params.split('=')
                    if ke == "keep":
                        keep = va.lower() == "true"
                    else:
                        logger.error("Invalid params. Expecting keep=<true or false>")
                        return False
                else:
                    logger.error("Invalid params. Expecting keep=<true or false>")
                    return False
            if up_services(services=args.services, keep=keep):
                return True
            else:
                logger.error("Error running one of the services")
                return False
        else:
            parser.print_help()
            msg = "\nServices of instances should be passed in the form of SERVICE=#instance1 SERVICE=#instances2\n"
            logger.error(msg)
            return False
    elif action == "status":
        combine_status()
        return True
    else:
        parser.print_help()
        return False


if __name__ == '__main__':
    parse_args()
