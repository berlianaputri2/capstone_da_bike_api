from flask import Flask, request
import sqlite3
import json
import pandas as pd

app = Flask(__name__)

@app.route('/')
def home():
    return 'Hello World'

@app.route('/stations/') 
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    conn.close()
    return stations.to_json()

def get_all_stations(conn):
    query = """SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    conn.close()
    return station.to_json()

@app.route('/trips/') 
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    conn.close()
    return trips.to_json()

def get_all_trips(conn):
    query = """SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

@app.route('/trips/<trip_id>')
def route_trip_id(trip_id):
    conn = make_connection()
    trip = get_trip_id(trip_id, conn)
    conn.close()
    return trip.to_json()

@app.route('/stations/add', methods=['POST'])
def route_add_station():
    data = pd.Series(json.loads(request.data))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_stations(data, conn)
    conn.close()
    return result

@app.route('/trips/add', methods=['POST'])
def route_add_trips():
    data = pd.Series(json.loads(request.data))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_trips(data, conn)
    conn.close()
    return result

@app.route('/json', methods=['POST'])
def json_example():
    req = request.get_json(force=True)
    name = req['name']
    age = req['age']
    address = req['address']

    return f'''Hello {name}, your age is {age}, and your address in {address}'''

@app.route('/trips/average_duration')
def trips_average_duration():
    conn = make_connection()
    trips = get_average_duration_trips(conn)
    conn.close()
    return trips.to_json()

@app.route('/trips/average_duration/<bikeid>')
def trips_average_duration_bike(bikeid):
    conn = make_connection()
    result = trips_average_duration_bike_query(bikeid, conn)
    conn.close()
    return result.to_json()

@app.route('/trips/trips_2018')
def trips_2018():
    conn = make_connection()
    result = trips_2018_query(conn)
    conn.close()
    return result.to_json()

############ Functions ############

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

def insert_into_stations(data, conn):
    query = """INSERT INTO stations VALUES (?, ?, ?, ...)"""
    try:
        conn.execute(query, data)
        conn.commit()
        return 'OK'
    except Exception as e:
        print(f"Error: {e}")
        return 'Error'

def insert_into_trips(data, conn):
    query = """INSERT INTO trips VALUES (?, ?, ?, ...)"""
    try:
        conn.execute(query, data)
        conn.commit()
        return 'OK'
    except Exception as e:
        print(f"Error: {e}")
        return 'Error'
    
def route_add_station():
    data = pd.Series(json.loads(request.data))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_stations(data, conn)
    conn.close()
    return result

def route_add_trips():
    data = pd.Series(json.loads(request.data))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_trips(data, conn)
    conn.close()
    return result


def get_average_duration_trips(conn):
    query = """SELECT AVG(duration_minutes) FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def trips_average_duration_bike_query(bikeid, conn):
    query = f"""SELECT AVG(duration_minutes) FROM trips WHERE bikeid IS {bikeid}"""
    result = pd.read_sql_query(query, conn)
    return result

def trips_2018_query(conn):
    query = """SELECT * FROM trips WHERE start_time LIKE '2018%'"""
    selected_data = pd.read_sql_query(query, conn)
    result = selected_data.groupby('start_station_id').agg({'bikeid': 'count', 'duration_minutes': 'mean'})
    return result

if __name__ == '__main__':
    app.run(debug=True, port=5000)
