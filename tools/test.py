from google.cloud import bigquery
from flask import Flask, request, jsonify

client = bigquery.Client()
table_id_base = "bdc-backend.city_simulator.{}"

table_id = table_id_base.format("census_shape_points")
table = client.get_table(table_id)

QUERY = (
        'SELECT * '
        'FROM `{}`').format(table_id)
query_job = client.query(QUERY)

for row in query_job:
    print(row)