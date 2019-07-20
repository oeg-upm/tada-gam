import os
import logging
from flask import Flask, g, request, jsonify, render_template, abort
import requests
import captain
import json
import random
import string
import shutil
from combine.graph.type_graph import TypeGraph

logging.basicConfig(level=logging.INFO)
LOG_LVL = logging.INFO
logger = logging.getLogger(__name__)

app = Flask(__name__)
upload_fname = 'local_uploads'
parent_path = os.sep.join(os.path.realpath(__file__).split(os.sep)[:-1])
UPLOAD_DIR = os.path.join(parent_path, upload_fname)
#UPLOAD_DIR = 'local_uploads'


def get_random(size=4):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(size))


@app.route('/')
def hello_world():
    return 'Hello World! This is the Captain!'


@app.route('/list')
def show_combine_list():
    ports_combine = captain.get_ports(service="combine")
    pairs = []
    for p in ports_combine:
        url = "http://127.0.0.1:"+p+"/list"
        print("url: "+url)
        response = requests.get(url)
        print(response.json())
        apples = response.json()["apples"]
        for apple in apples:
            pairs.append({'url': url, 'apple': apple, 'port': p})
            # pairs.append((url, apple))
        # combines[url] = response.json()["apples"]
        # apples += response.json()["apples"]
    print("pairs: ")
    print(pairs)
    return render_template('list_apples.html', pairs=pairs)


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


def get_labels_from_graph(graph_dir, m, alpha, fsid):
    # graph_dir = os.path.join(UPLOAD_DIR, graph_dir)
    f = open(graph_dir, 'r')
    j = json.loads(f.read())
    g = TypeGraph()
    g.load(j, m)
    g.set_score_for_graph(coverage_weight=alpha, m=m, fsid=fsid)
    return g, [n.title for n in g.get_scores()]


def get_nodes(graph):
    return [graph.index[t] for t in graph.cache]


def get_graph(port, apple_id):
    url = "http://127.0.0.1:" + port + "/get_graph?id="+apple_id
    # result = request.get(url)
    print("get graph from url: "+url)
    r = requests.get(url, stream=True)
    dest_path = os.path.join(UPLOAD_DIR, str(apple_id)+"__"+get_random(12)+"_graph.json")
    # dest_path = os.path.join(UPLOAD_DIR, str(apple_id)+"_graph.json")
    if r.status_code == 200:
        with open(dest_path, 'wb') as f:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, f)
        return dest_path
    else:
        print(r.content)
    return None


if __name__ == '__main__':
    if 'port' in os.environ:
        app.run(debug=True, host='0.0.0.0', port=int(os.environ['port']))
    else:
        app.run(debug=True, host='0.0.0.0')

