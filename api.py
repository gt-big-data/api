from flask import Flask, request, jsonify
from google.cloud import bigquery
import requests
import networkx as nx
import json

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

@app.route("/weather/<string:loc>", methods=['GET'])
def get_weather(loc):
    url = "http://api.openweathermap.org/data/2.5/weather?q=" + loc
    url += "&APPID="
    credentials = open("credentials.txt")
    key = credentials.readline()
    url += key
    r = requests.get(url)
    data = r.json()
    return data

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

@app.route("/path/<int:id_1>:<int:id_2>", methods=['GET'])
def path(id_1, id_2):
    table_id = table_id_base.format("streets")
    table = client.get_table(table_id)
    QUERY = (
        'SELECT * '
        'FROM `{}` ').format(table_id)
    query_job = client.query(QUERY)
    rows = [dict(row) for row in query_job.result()]
    if request.method == 'GET':
        
        graph = {}
        table =  jsonify(rows)
        G = nx.Graph()


        for object in rows:
            if object["node_id_start"] not in graph:
                
                AdjNode = (object["node_id_end"], object["true_duration_s"])
                val = [AdjNode]
                graph[object["node_id_start"]] = val
                G.add_edge(object["node_id_start"], object["node_id_end"], weight=object["true_duration_s"])


            curAdjNode = (object["node_id_end"], object["true_duration_s"])
            graph[object["node_id_start"]].append(curAdjNode)
            G.add_edge(object["node_id_start"], object["node_id_end"], weight=object["true_duration_s"])



        if id_1 not in graph or id_2 not in graph:
            return "<h3>Invalid Source or Destination</h3>"

        if nx.has_path(G, id_1, id_2):
            output = "<h3>The shortest path from " + str(id_1) + " to " + str(id_2) + " is:</h3> <br />"
            output += str(nx.shortest_path(G, id_1, id_2, weight='weight')) + " <br />"
            output += "<h3>Cost (Seconds):</h3>" + str(nx.shortest_path_length(G, id_1, id_2, weight='weight'))

        else:
            output = "<h3>No path exists :("
        
        

        return output


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
