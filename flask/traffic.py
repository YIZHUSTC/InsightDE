from flask import Flask, g, redirect, url_for, abort, render_template, request, jsonify
from shapely.geometry import shape, Point
from datetime import datetime, timedelta
import json, time, os, re
import numpy as np
from collections import Counter
import psycopg2


app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    points = dict()
    points = []

    # All high traffic volume locations
    connection = psycopg2.connect(host = '127.0.0.1', database = 'postgres', user = 'postgres', password = 'postgres')
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM realtimetraffic WHERE level = %s;', ('High',))
    result = cursor.fetchall()
    for x in result:
        points.append({"latitude": x[2], "longitude": x[3], "extra": x[8]-x[9]})
    return render_template("index.html", title="TrafficAdvisor: Real-Time Traffic Monitoring", points=points)




@app.route('/search', methods=['GET'])
def search():

    results = dict()

    # Nearest location
    connection = psycopg2.connect(host = '127.0.0.1', database = 'postgres', user = 'postgres', password = 'postgres')
    cursor = connection.cursor()
    coordinate = request.args.get('coordinate')
    latitude=coordinate.split(",")[0]
    longitude=coordinate.split(",")[1]


    point = 'POINT('+str(latitude)+' '+str(longitude)+')'
    cursor.execute('SELECT *, st_distance(ST_GeomFromText(%s, 4326), realtimetraffic.geom) \
        as distance FROM realtimetraffic ORDER BY distance LIMIT 1;', (point,))
    query = cursor.fetchone()
    results['location'] = query[1]
    results['latitude'] = query[2]
    results['longitude'] = query[3]
    results['direction'] = query[4]
    results['lane'] = query[5]
    results['type'] = query[6]
    results['highway'] = query[7]
    results['current'] = query[8]
    results['historical'] = round(query[9])
    results['predicted'] = query[10]
    results['level'] = query[11]
    
    return jsonify(results)


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=8080)
