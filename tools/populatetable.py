from google.cloud import bigquery
from flask import Flask, request, jsonify
import time as time

client = bigquery.Client()
table_id_base = "bdc-backend.city_simulator.{}"

table_id = table_id_base.format("census_shape_points")
table = client.get_table(table_id)
f = open("points_table.csv")
print(f.readline())

rows_to_insert = list()



for i in range(115):
    rows_to_insert = list()
    for line in range(9999):
        i = f.readline()
        i = i.strip()
        i = i.split(",")
        if (i != []):
            i[2] = int(i[2].strip('"'))
            i[4] = float(i[4].strip('"'))
            i[5] = float(i[5].strip('"'))
        rows_to_insert.append((i[3], i[1], i[4], i[5]))
    errors = client.insert_rows(table, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    time.sleep(1)
