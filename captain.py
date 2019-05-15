import argparse
import subprocess
import random


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
    print("combine> file: %s, col: %d, port: %s" % (file_dir, col, port))
    num_ports = len(score_ports)
    i = random.randint(0, num_ports-1)
    # assume to have 3 slices
    for x in range(3):
        print("score> file: %s, col: %d, slice: %d, score_port: %s, combine_port: %s" % (file_dir, col, x, score_ports[x], port))
        i = i + 1
        i = i % num_ports


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
            label_column(file_dir=f, col=c, slice_size=slice_size, port=ports_combine[i], score_ports=ports_score)
            i = i+1
            i = i % num_ports


def parse_args():
    parser = argparse.ArgumentParser(description='Captain to look after the processes')
    parser.add_argument('action', help='What action you like to perform', choices=['label', 'ports'])
    parser.add_argument('--files', nargs='+', help="The set of file to be labeled")
    # parser.add_argument('--cols', nargs='+', help="The indices of the columns (starting from 0)")
    parser.add_argument('--slicesize', help="The max number of elements in a slice", type=int, default=10)
    # parser.add_argument('--dir', help="The directory of the input files to be labeled")
    args = parser.parse_args()
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
    else:
        parser.print_help()


if __name__ == '__main__':
    parse_args()