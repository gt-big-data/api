from flask import Flask, request, jsonify
from google.cloud import bigquery


app = Flask(__name__)

client = bigquery.Client()
table_id_base = "bdc-backend.city_simulator.{}"


@app.route("/intersections", methods=['GET'])
def get_intersections():
    table_id = table_id_base.format("intersections")
    table = client.get_table(table_id)
    QUERY = (
        'SELECT * '
        'FROM `{}` ').format(table_id)
    query_job = client.query(QUERY)
    rows = [dict(row) for row in query_job.result()]
    if request.method == 'GET':
        return jsonify(rows)
    else:
        return "405: Restricted method"


@app.route("/intersections/<int:id>", methods=['GET'])
def get_intersections_by_id(id):
    table_id = table_id_base.format("intersections")
    table = client.get_table(table_id)
    QUERY = (
        'SELECT * '
        'FROM `{}` '
        'WHERE node_id={} ').format(table_id, id)
    query_job = client.query(QUERY)
    rows = [dict(row) for row in query_job.result()]
    if request.method == 'GET':
        return jsonify(rows[0])
    else:
        return "405: Restricted method"


@app.route("/streets", methods=['GET'])
def get_streets():
    table_id = table_id_base.format("streets")
    table = client.get_table(table_id)
    QUERY = (
        'SELECT * '
        'FROM `{}` ').format(table_id)
    query_job = client.query(QUERY)
    rows = [dict(row) for row in query_job.result()]
    if request.method == 'GET':
        return jsonify(rows)
    else:
        return "405: Restricted method"


@app.route("/streets/<int:id_1>:<int:id_2>", methods=['GET'])
def get_streets_by_id(id_1, id_2):
    table_id = table_id_base.format("streets")
    table = client.get_table(table_id)
    QUERY = (
        'SELECT * '
        'FROM `{}` '
        'WHERE node_id_start={} '
        'AND node_id_end={}').format(table_id, id_1, id_2)
    query_job = client.query(QUERY)
    rows = [dict(row) for row in query_job.result()]
    if request.method == 'GET':
        return jsonify(rows[0])
    else:
        return "405: Restricted method"


if __name__ == '__main__':
    app.run()
