import argparse
import sys
from combine.app import merge_graphs
from captain import get_ports
from app import get_random, get_graph, get_labels_from_graph, get_nodes
import requests
import logging
import os
from flask import Flask, render_template, request, abort
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('debug_tools')


app = Flask(__name__)
upload_fname = 'local_uploads'
parent_path = os.sep.join(os.path.realpath(__file__).split(os.sep)[:-1])
UPLOAD_DIR = os.path.join(parent_path, upload_fname)


@app.route('/list')
def show_bites_list():
    ports_score = get_ports(service="score")
    bites_dict = dict()
    for p in ports_score:
        url = "http://127.0.0.1:"+p+"/list"
        response = requests.get(url)
        print(response.json())
        bites = response.json()["bites"]
        for b in bites:
            k = b["table"]
            if k not in bites_dict:
                bites_dict[k] = dict()
            k2 = b["slice"]
            if k2 not in bites_dict[k]:
                bites_dict[k][k2] = p
            else:
                raise Exception("This table, slice combination is already set. Dupicates are not allowed")

    ports_combine = get_ports(service="combine")
    pairs = []
    for p in ports_combine:
        url = "http://127.0.0.1:"+p+"/list_bites"
        print("url: "+url)
        response = requests.get(url)
        print(response.json())
        bites = response.json()["bites"]
        for bite in bites:
            pairs.append({'url': url, 'bite': bite, 'port': bites_dict[bite["apple"]["table"]][bite["slice"]]})
            # pairs.append((url, apple))
        # combines[url] = response.json()["apples"]
        # apples += response.json()["apples"]
    print("pairs: ")
    print(pairs)
    if len(pairs) == 0:
        return "No processed files in any combine instance"
    return render_template('list_bites.html', pairs=pairs)


@app.route('/get_label', methods=["GET"])
def get_label():
    port = request.values.get('port')
    apple_id = request.values.get('id')
    m = int(request.values.get('m'))
    graph_dir = get_graph(port=port, apple_id=apple_id)
    if graph_dir:
        print("graph_dir is found: "+str(graph_dir))
        alpha = 0.01
        alpha_passed = request.args.get("alpha")
        if alpha_passed is not None:
            alpha = float(alpha_passed)

        fsid = 3
        fsid_passed = request.args.get("fsid")
        if fsid_passed is not None:
            fsid = int(fsid_passed)
        print("graph_dir before get labels: "+graph_dir)
        g, labels = get_labels_from_graph(graph_dir=graph_dir, m=m, alpha=alpha, fsid=fsid)

        # return render(request, 'ent_ann_recompute.html',
        #               {'anns': eanns, 'alpha': alpha, 'network': 'network',
        #                'highlights': results[:3], 'nodes': get_nodes(g), 'fsid': fsid,
        #                'edges': annotator.get_edges(graph), 'results': results, 'selected': entity_ann.id})

        return render_template('labels.html', labels=labels, network='network', highlights=labels[:3],
                               nodes=get_nodes(g), fsid=fsid, edges=g.get_edges(), results=labels,
                               port=port, apple_id=apple_id, m=m, alpha=alpha)
    logger.error("No graph")
    abort(500)


def get_slice(fdir, slice_size, slice_num, col_id):
    """
    :param fdir:
    :param slice_size:
    :param slice_num:
    :param col_id:
    :return:
    """
    df = pd.read_csv(fdir)
    dfcol = df.iloc[:, col_id]
    content = "\t".join(dfcol[slice_size*slice_num:slice_size*(slice_num+1)].astype(str).values.tolist())
    f = open("slice.tsv", "w")
    f.write(content)
    f.close()


def get_graphs(table):
    ports = get_ports("score")
    fname = table + "__" + get_random()
    for p in ports:
        host = "http://127.0.0.1:%s" % str(p)
        response = requests.get(host+"/list")
        j = response.json()
        for bite in j["bites"]:
            if bite["table"] == table:
                response = requests.get(host+"/get_graph?id=%d" % bite["id"])
                fdir = 'local_uploads/%s__%d.json' % (fname, bite["id"])
                with open(fdir, 'w') as f:
                    f.write(response.content)
                logger.info("fetch: %s" % (fdir))


def parse_args(args=None):
    parser = argparse.ArgumentParser(description='Debug tools to help in the process')
    parser.add_argument('action', help='What action you like to perform', choices=['get_graphs', 'merge_graphs',
                                                                                   'show_bites', 'get_slice'])
    parser.add_argument('--table', help="The name of the table")
    parser.add_argument('--file', help="The input file")
    parser.add_argument('--slicesize', help="The max number of elements in a slice", type=int, default=10)
    parser.add_argument('--slicenum', help="The number of the slice", type=int, default=0)
    parser.add_argument('--colid', help="The order of the column", type=int, default=0)

    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)

    if args.action == "get_graphs":
        if args.table:
            get_graphs(args.table)
        else:
            parser.print_help()
    elif args.action == "merge_graphs":
        if args.table:
            pass
    elif args.action == "show_bites":
        if 'debug_port' in os.environ:
            app.run(debug=True, host='0.0.0.0', port=int(os.environ['debug_port']))
        else:
            app.run(debug=True, host='0.0.0.0')
    elif args.action == "get_slice":
        if args.file:
            get_slice(fdir=args.file, slice_size=args.slicesize, slice_num=args.slicenum, col_id=args.colid)
        else:
            parser.print_help()
    else:
        parser.print_help()


if __name__ == "__main__":
    parse_args()