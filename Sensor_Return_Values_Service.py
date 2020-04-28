from flask import Flask, request, jsonify
import psycopg2
from prometheus_flask_exporter import PrometheusMetrics

app = Flask(__name__)

metrics = PrometheusMetrics(app)

metrics.info('app_info_sensor_Return', 'Application info', version='1.0.0')


@app.before_first_request
def before_first_request_func():

    db = psycopg2.connect(host='db_sensors', port=5432, user='postgres', password='postgres', dbname='sensors_info_db')

    cursor = db.cursor()
    cursor.execute(
        """
        CREATE TABLE if not exists sensors_values (
                id SERIAL PRIMARY KEY,
                sensor_type VARCHAR(20) NOT NULL,
                sensor_timestamp TIMESTAMP NOT NULL,
                sensor_value NUMERIC (12, 6) NOT NULL

        )
        """)
    if cursor is not None:
        cursor.close()

    db.commit()
    if db is not None:
        db.close()


def query_single_values(sensorType, methodType, beginningPeriod, endingPeriod):
    
    db = psycopg2.connect(host='db_sensors', port=5432, user='postgres', password='postgres', dbname='sensors_info_db')
    cursor = db.cursor()

    # Default if nothing is returned from select. It is used only when sending the json back for compatibility
    # If the select returns a value it will be updated and used
    value = -1000000
    successful = True
    record = []

    try:
        if methodType == "Average":
            cursor.execute("select round(avg(sensor_value), 6) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s", (sensorType, beginningPeriod, endingPeriod)) 
        elif methodType == "Smallest":
            cursor.execute("select min(sensor_value) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s", (sensorType, beginningPeriod, endingPeriod))
        elif methodType == "Biggest":
            cursor.execute("select max(sensor_value) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s", (sensorType, beginningPeriod, endingPeriod))
        elif methodType == "Oldest":
            cursor.execute("select sensor_value from sensors_values where id = (select min(id) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s)", (sensorType, beginningPeriod, endingPeriod)) 
        elif methodType == "Newest":
            cursor.execute("select sensor_value from sensors_values where id = (select max(id) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s)", (sensorType, beginningPeriod, endingPeriod)) 

        record = cursor.fetchall()

    except:
        successful = False

    size = 0
    if (len(record) > 0 and record[0][0] is not None):
        value = float(record[0][0])
        size = len(record)

    if successful:
        jsonResponse = jsonify({"status" : "Success", "size" : size, "value" : float(value)})
    else:
        jsonResponse = jsonify({"status" : "Failed", "size" : 0, "value" : float(value)})        

    if cursor is not None:
        cursor.close()
    if db is not None:
        db.close()

    return jsonResponse

def query_multiple_values(sensorType, methodType, beginningPeriod, endingPeriod, methodPerInteval):
    db = psycopg2.connect(host='db_sensors', port=5432, user='postgres', password='postgres', dbname='sensors_info_db')
    cursor = db.cursor()

    successful = True
    records = []

    try:

        if methodPerInteval == "All":
            cursor.execute("select sensor_timestamp, sensor_value from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s", (sensorType, beginningPeriod, endingPeriod))
        else:
            if methodPerInteval == "Day":

                if methodType == "Average":
                    cursor.execute("select sensor_timestamp::date, round(avg(sensor_value),2) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by sensor_timestamp::date order by sensor_timestamp::date", (sensorType, beginningPeriod, endingPeriod))
                elif methodType == "Smallest":
                    cursor.execute("select sensor_timestamp::date, round(min(sensor_value),2) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by sensor_timestamp::date order by sensor_timestamp::date", (sensorType, beginningPeriod, endingPeriod))
                elif methodType == "Biggest":
                    cursor.execute("select sensor_timestamp::date, round(max(sensor_value),2) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by sensor_timestamp::date order by sensor_timestamp::date", (sensorType, beginningPeriod, endingPeriod))
            
            elif methodPerInteval == "Year":
                if methodType == "Average":
                    cursor.execute("select extract(%s from sensor_timestamp), round(avg(sensor_value),3) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by extract(%s from sensor_timestamp) order by extract(%s from sensor_timestamp)", (methodPerInteval, sensorType, beginningPeriod, endingPeriod, methodPerInteval, methodPerInteval))
                elif methodType == "Smallest":
                    cursor.execute("select extract(%s from sensor_timestamp), round(min(sensor_value),3) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by extract(%s from sensor_timestamp) order by extract(%s from sensor_timestamp)", (methodPerInteval, sensorType, beginningPeriod, endingPeriod, methodPerInteval, methodPerInteval))
                elif methodType == "Biggest":
                    cursor.execute("select extract(%s from sensor_timestamp), round(max(sensor_value),3) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by extract(%s from sensor_timestamp) order by extract(%s from sensor_timestamp)", (methodPerInteval, sensorType, beginningPeriod, endingPeriod, methodPerInteval, methodPerInteval))
            
            elif methodPerInteval == "Week":
                if methodType == "Average":
                    cursor.execute("select to_char(sensor_timestamp, 'IYYY-IW'), round(avg(sensor_value),3) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by to_char(sensor_timestamp, 'IYYY-IW') order by to_char(sensor_timestamp, 'IYYY-IW')", (sensorType, beginningPeriod, endingPeriod))
                elif methodType == "Smallest":
                    cursor.execute("select to_char(sensor_timestamp, 'IYYY-IW'), round(min(sensor_value),3) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by to_char(sensor_timestamp, 'IYYY-IW') order by to_char(sensor_timestamp, 'IYYY-IW')", (sensorType, beginningPeriod, endingPeriod))
                elif methodType == "Biggest":
                    cursor.execute("select to_char(sensor_timestamp, 'IYYY-IW'), round(max(sensor_value),3) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by to_char(sensor_timestamp, 'IYYY-IW') order by to_char(sensor_timestamp, 'IYYY-IW')", (sensorType, beginningPeriod, endingPeriod))

            elif methodPerInteval == "Month":
                if methodType == "Average":
                    cursor.execute("select to_char(sensor_timestamp, 'YYYY-MM'), round(avg(sensor_value),3) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by to_char(sensor_timestamp, 'YYYY-MM') order by to_char(sensor_timestamp, 'YYYY-MM')", (sensorType, beginningPeriod, endingPeriod))
                elif methodType == "Smallest":
                    cursor.execute("select to_char(sensor_timestamp, 'YYYY-MM'), round(min(sensor_value),3) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by to_char(sensor_timestamp, 'YYYY-MM') order by to_char(sensor_timestamp, 'YYYY-MM')", (sensorType, beginningPeriod, endingPeriod))
                elif methodType == "Biggest":
                    cursor.execute("select to_char(sensor_timestamp, 'YYYY-MM'), round(max(sensor_value),3) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by to_char(sensor_timestamp, 'YYYY-MM') order by to_char(sensor_timestamp, 'YYYY-MM')", (sensorType, beginningPeriod, endingPeriod))

        records = cursor.fetchall()

    except:
        successful = False

    list_of_values = []
    if (len(records) > 0):
        for raw in records:
            list_of_values.append({"x" : str(raw[0]), "y" : float(raw[1])})
    
    if successful:
        jsonResponse = jsonify({"status" : "Success", "size" : len(records), "values" : list_of_values})
    else:
        jsonResponse = jsonify({"status" : "Failed", "size" : 0, "values" : list_of_values})        

    if cursor is not None:
        cursor.close()
    if db is not None:
        db.close()

    return jsonResponse


@app.route('/', methods=['POST'])
def take_data():

    jsonData = request.get_json()
    returnType = jsonData['returntype']
    sensorType = jsonData['sensortype']
    methodType = jsonData['method']
    beginningPeriod = jsonData['beginningperiod']
    endingPeriod = jsonData['endingperiod']
    methodPerInteval = jsonData['methodperinterval']

    # Tipul senzorului nu trebuie parsat pentru ca il dau functiei direct si in db o sa se faca interogarea doar pentru tipul respectiv de senzor
    if returnType == "singlevalue":
        jsonResponse = query_single_values(sensorType, methodType, beginningPeriod, endingPeriod)

    elif returnType == "multiplevalues":
        jsonResponse = query_multiple_values(sensorType, methodType, beginningPeriod, endingPeriod, methodPerInteval)

    return jsonResponse

if __name__ == "__main__":
    app.run(host="0.0.0.0")