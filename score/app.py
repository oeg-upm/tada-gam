from models import Bite, database, create_tables
from flask import Flask, g, request, render_template
from werkzeug.utils import secure_filename
from graph import type_graph

import requests


app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World! graph'


@app.route('/score', methods=['POST'])
def score():
    print(request.files)
    uploaded_file = request.files['file_slice']
    print("post data:" )
    print request.form
    print("file content: ")
    print(uploaded_file.read())
    b = Bite(table=request.form['table'], slice=request.form['slice'], column=request.form['column'], addr=request.form['addr'])
    b.save()
    get_params = {
        'table': b.table,
        'column': b.column,
        'slice': b.slice,
        'addr': b.addr,
        'total': request.form['total']
    }
    print("address: "+str(request.form['addr']+"/add"))
    requests.get(request.form['addr']+"/add", params=get_params)
    return 'data received and processed'


@app.route('/register', methods=['GET'])
def register():
    table_name = request.args.get('table')
    # Bite.create(table=table_name, slice=0, column=0)
    b = Bite(table=table_name, slice=0, column=0, addr="default")
    b.save()
    return 'Table: %s is added' % table_name


@app.route('/fetch', methods=['GET'])
def fetch():
    bites = """
    Bites
    <table>
    """
    for bite in Bite.select():
        bites += "<tr>"
        bites += "<td>%s</td>\n" % bite.table
        bites += "<td>%d</td>\n" % bite.column
        bites += "<td>%d</td>\n" % bite.slice
        bites += "<td>%s</td>\n" % bite.addr
        bites += "</tr>"
    bites += "</table>"
    return bites


@app.before_request
def before_request():
    g.db = database
    g.db.connect()


@app.after_request
def after_request(response):
    g.db.close()
    return response


if __name__ == '__main__':
    create_tables()
    app.run(debug=True, host='0.0.0.0')




