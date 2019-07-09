import argparse
import subprocess
import random
import os
import requests
import pandas as pd
import io

# This is the default network created by docker
NETWORK = "tada-gam_default"


def get_network_ip():
    """
    Get the ip of the network
    :return:
    """
    comm = """ docker network inspect tada-gam_default | grep "Gateway" """
    # print("comm: "+comm)
    a = subprocess.Popen(comm, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()[0]
    ip = a.split(":")[1].replace('"', '').strip()
    # print("The ip: "+ip)
    return ip


def get_ports(service):
    a = subprocess.check_output(["docker-compose", "ps", service])
    print(a)
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
    print("\n\ncombine> file: %s, col: %d, port: %s" % (file_dir, col, port))
    COMBINE_HOST = "http://"+get_network_ip()
    num_ports = len(score_ports)
    i = random.randint(0, num_ports-1)
    df = pd.read_csv(file_dir)
    dfcol = df.iloc[:, 0]
    fname = file_dir.split(os.sep)[-1]
    total_num_slices = dfcol.shape[0]/slice_size
    if dfcol.shape[0]%slice_size != 0:
        total_num_slices += 1
    score_port_idx = random.randint(0, num_ports-1)
    for slice_idx in range(total_num_slices):
        score_port = score_ports[(score_port_idx+slice_idx)%num_ports]
        print("score> file: %s, col: %d, slice: %d, score_port: %s, combine_port: %s" % (file_dir, col, slice_idx,
                                                                                         score_port, port))
        slice_from = slice_idx*slice_size
        #slice_to = slice_from + slice_size
        slice_to = min(slice_from + slice_size, dfcol.shape[0]-1)  # to cover the cases where the last slice is not full
        # files = {'file_slice': open(file_dir, 'rb')}
        files = {'file_slice': (fname, "\t".join(dfcol[slice_from:slice_to].values.tolist()))}
        values = {'table': fname, 'column': col, 'slice': slice_idx, 'total': total_num_slices,
                  'addr': COMBINE_HOST+":"+str(port)}
        score_url = "http://127.0.0.1:"+str(score_port)+"/score"
        # print("files: "+str(files))
        # print("post data: "+str(values))
        # print("score url: "+str(score_url))
        r = requests.post(score_url, files=files, data=values)
        if r.status_code != 200:
            print("error: "+str(r.content))
        i = i + 1
        i = i % num_ports


def up_services(services):
    """
    :param services: list of strings
    :return:
    """

    # Shutdown running instances
    comm = "docker-compose down"
    subprocess.call(comm, shell=True)

    # # Rebuild images
    # comm = "docker-compose build"
    # subprocess.call(comm, shell=True)

    # Running services
    port = 5100
    for service_instance in services:
        if '=' in service_instance:
            service, instances = service_instance.split('=')
        else:
            instances = 1
            service = service_instance

        for i in range(int(instances)):
            in_port = port
            # if service == "combine":
            #     in_port = port
            # else:
            #     in_port = 5000
            if run_service(service, port, in_port):
                port += 1
            else:
                print("Error in running service: %s" % (service))
                return False

    comm = "docker-compose ps"
    subprocess.call(comm, shell=True)
    return True

# def up_services(services, instances):
#     """
#     :param services: list of strings
#     :param instances: list of instances
#     :return:
#     """
#
#     # Shutdown running instances
#     comm = "docker-compose down"
#     subprocess.call(comm, shell=True)
#
#     # Rebuild images
#     comm = "docker-compose build"
#     subprocess.call(comm, shell=True)
#
#     # Running services
#     port = 5100
#     for idx in range(len(services)):
#         services[idx]
#         instances[idx]
#         for i in range(int(instances[idx])):
#             if services[idx] == "combine":
#                 in_port = port
#             else:
#                 in_port = 5000
#             if run_service(services[idx], port, in_port):
#                 port += 1
#             else:
#                 print("Error in running service: %s" % (services[idx]))
#                 return False
#
#     comm = "docker-compose ps"
#     subprocess.call(comm, shell=True)
#     return True


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


def label_files(files, slice_size):
    """
    :param files: the directory of the files
    :return:
    """
    # print("files: " + str(files))
    # ports_combine = get_ports(service="combine")
    # ports_score = get_ports(service="score")
    # print("combine: " + str(ports_combine))
    # print("score: " + str(ports_score))
    # elected_combine = random.choice(ports_combine)
    # print("random picked score: " + random.choice(ports_score))
    ports_combine = get_ports(service="combine")
    ports_score = get_ports(service="score")
    num_ports = len(ports_combine)
    i = random.randint(0, num_ports-1)
    for f in files:
        for c in detect_entity_cols(f):
            print("label_column in file: "+f)
            label_column(file_dir=f, col=c, slice_size=slice_size, port=ports_combine[i], score_ports=ports_score)
            i = i+1
            i = i % num_ports


def parse_args(args=None):
    parser = argparse.ArgumentParser(description='Captain to look after the processes')
    parser.add_argument('action', help='What action you like to perform', choices=['label', 'ports', 'up', 'status'])
    parser.add_argument('--files', nargs='+', help="The set of file to be labeled")
    # parser.add_argument('--cols', nargs='+', help="The indices of the columns (starting from 0)")
    parser.add_argument('--slicesize', help="The max number of elements in a slice", type=int, default=10)
    # parser.add_argument('--instances', nargs='+', help="The numbers of instances (as a list)")
    parser.add_argument('--services', nargs='+', help="The names of the services")
    # parser.add_argument('--dir', help="The directory of the input files to be labeled")
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
        # print("random picked score: "+random.choice(ports_score))
    elif action == "label":
        if args.files:
            label_files(files=args.files, slice_size=args.slicesize)
            # if args.cols and len(args.files) != len(args.cols):
            #         parser.print_help()
            #         print("\n\nERROR: Number of indices need to match the number of files\n")
            #         return
            # else:
            #     label_files(files=args.files, cols=args.cols, slice_size=args.slicesize)
        else:
            parser.print_help()
    elif action == "up":
        if args.services:
            if up_services(args.services):
                pass
            else:
                print("Error running one of the services")
        else:
            parser.print_help()
            msg = "\nServices of instances should be passed in the form of SERVICE=#instance1 SERVICE=#instances2\n"
            print(msg)
    elif action == "status":
        combine_status()
        # if args.instances and args.services:
        #     if len(args.instances) != len(args.services):
        #         parser.print_help()
        #         msg = "\nThe number of services and the number of instances should match\n"
        #         print(msg)
        #     else:
        #         if up_services(args.services, args.instances):
        #             pass
        #         else:
        #             print("Error running one of the services")
        #             #parser.print_help()
        # else:
        #     parser.print_help()
        #     msg = "\nServices and the number of instances should be passed\n"
        #     print(msg)
    else:
        parser.print_help()


if __name__ == '__main__':
    parse_args()
