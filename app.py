import os
import logging
from flask import Flask, g, request, jsonify, render_template
import requests
import captain

logging.basicConfig(level=logging.INFO)
LOG_LVL = logging.INFO
logger = logging.getLogger(__name__)

app = Flask(__name__)
UPLOAD_DIR = 'local_uploads'


@app.route('/')
def hello_world():
    return 'Hello World! This is the Captain!'


@app.route('/list')
def show_combine_list():
    ports_combine = captain.get_ports(service="combine")
    pairs = []
    for p in ports_combine:
        url = "http://127.0.0.1:"+p+"/status"
        print("url: "+url)
        response = requests.get(url)
        apples = response.json()["apples"]
        for apple in apples:
            pairs.append({'url': url, 'apple': apple})
            # pairs.append((url, apple))
        # combines[url] = response.json()["apples"]
        # apples += response.json()["apples"]
    return render_template('list_apples.html', pairs=pairs)


if __name__ == '__main__':
    if 'port' in os.environ:
        app.run(debug=True, host='0.0.0.0', port=int(os.environ['port']))
    else:
        app.run(debug=True, host='0.0.0.0')

