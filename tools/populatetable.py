from google.cloud import bigquery
from flask import Flask, request, jsonify

client = bigquery.Client()
table_id_base = "bdc-backend.city_simulator.{}"

table_id = table_id_base.format("census_shape_points")
table = client.get_table(table_id)
f = open("points_table.csv")
f.readline()

rows_to_insert = list()
##### census shape points
# point id: integer
# centroid id: integer


for line in range(5):
    i = f.readline()
    i = i.strip()
    i = i.split(",")
    print(i)


#errors = client.insert_rows(table, rows_to_insert)  # Make an API request.
#if errors == []:
#    print("New rows have been added.")

#for line in range(9999):
#    i = f.readline()
#    i = i.strip()
#    i = i.split(",")
#    if (i != []):
#        rows_to_insert.append((i[1].strip('"'), float(i[3]), float(i[4]), 0))


#errors = client.insert_rows(table, rows_to_insert)  # Make an API request.
#if errors == []:
#    print("New rows have been added.")
    

#for line in range(9999):
#    i = f.readline()
#    i = i.strip()
#    i = i.split(",")
#    if (i != []):
#        rows_to_insert.append((i[1].strip('"'), float(i[3]), float(i[4]), 0))


#errors = client.insert_rows(table, rows_to_insert)  # Make an API request.
#if errors == []:
#    print("New rows have been added.")
    

#for line in range(9999):
#    i = f.readline()
#    i = i.strip()
#    i = i.split(",")
#    if (i != []):
#        try:
#            rows_to_insert.append((i[1].strip('"'), float(i[3]), float(i[4]), 0))
#        except:
#            pass


#errors = client.insert_rows(table, rows_to_insert)  # Make an API request.
#if errors == []:
#    print("New rows have been added.")
    

