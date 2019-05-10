from models import Bite, database, create_tables
from flask import Flask, g, request, render_template
from graph import type_graph

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World! graph'


@app.route('/register', methods=['GET'])
def register():
    table_name = request.args.get('table')
    # Bite.create(table=table_name, slice=0, column=0)
    b = Bite(table=table_name, slice=0, column=0, target="default")
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
        bites += "<td>%s</td>\n" % bite.target
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




