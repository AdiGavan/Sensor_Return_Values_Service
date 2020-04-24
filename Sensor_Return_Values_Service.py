from flask import Flask, request, jsonify
import psycopg2
from prometheus_flask_exporter import PrometheusMetrics

app = Flask(__name__)

metrics = PrometheusMetrics(app)

metrics.info('app_info_sensor_Return', 'Application info', version='1.0.3')


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

    if (len(record) > 0):
        value = record[0][0]
    if successful:
        #jsonResponse = jsonify({"status" : "Success", "size" : len(record), "value" : float(value)})
        jsonResponse = jsonify({"status" : "Success", "size" : len(record), "value" : record})
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

    #try:

    if methodType == "all":
        cursor.execute("select sensor_timestamp, sensor_value from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s", (sensorType, beginningPeriod, endingPeriod))
    else:
        if methodPerInteval == "day":

            if methodType == "Average":
                cursor.execute("select sensor_timestamp::date, round(avg(sensor_value),2) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by sensor_timestamp::date order by sensor_timestamp::date", (sensorType, beginningPeriod, endingPeriod))
            elif methodType == "Smallest":
                cursor.execute("select sensor_timestamp::date, round(min(sensor_value),2) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by sensor_timestamp::date order by sensor_timestamp::date", (sensorType, beginningPeriod, endingPeriod))
            elif methodType == "Biggest":
                cursor.execute("select sensor_timestamp::date, round(max(sensor_value),2) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by sensor_timestamp::date order by sensor_timestamp::date", (sensorType, beginningPeriod, endingPeriod))
        
        else:
            if methodType == "Average":
                cursor.execute("select extract(%s from sensor_timestamp), round(avg(sensor_value),3) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by extract(%s from sensor_timestamp) order by extract(%s from sensor_timestamp)", (methodPerInteval, sensorType, beginningPeriod, endingPeriod, methodPerInteval, methodPerInteval))
            elif methodType == "Smallest":
                cursor.execute("select extract(%s from sensor_timestamp), round(min(sensor_value),3) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by extract(%s from sensor_timestamp) order by extract(%s from sensor_timestamp)", (methodPerInteval, sensorType, beginningPeriod, endingPeriod, methodPerInteval, methodPerInteval))
            elif methodType == "Biggest":
                cursor.execute("select extract(%s from sensor_timestamp), round(max(sensor_value),3) from sensors_values where sensor_type = %s and sensor_timestamp >= %s and sensor_timestamp <= %s group by extract(%s from sensor_timestamp) order by extract(%s from sensor_timestamp)", (methodPerInteval, sensorType, beginningPeriod, endingPeriod, methodPerInteval, methodPerInteval))

    records = cursor.fetchall()
        #value = record[0][0]

    #except:
        #successful = False

    list_of_values = []
    if (len(records) > 0):
        for raw in records:
            list_of_values.append({str(raw[0]) : float(raw[1])})
    
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
    app.run(host="0.0.0.0", debug=True)



    # ---------------- Comentarii logica parsare (amintit pentru etapa 2) -------------------


    # Test postman:

"""
    {
	    "ReturnType" : "multiple_values", "SensorType" : "Pressure", "Method" : "smallest_value", "PeriodType" : "week", "NumberOfPeriod" : "7"
    }
"""

    # 1. Mai intai verific daca e pentru statistica sau doar pentru valoare => ReturnType : single_value sau multiple_values
    # 2. Dupa verific dupa tip senzor => SensorType : Temperature, BoardTemperature, Pressure, Humidity, AirQuality, LightIntensity
    
    # 3. Pentru single_value:   

    # 3.1. Method :

    # a. mean_value
    # b. smalles_value
    # c. biggest_value
    # d. oldest_value
    # e. newest_value
    # f. specific_moment -> pentru asta nu conteaza period type, doar ca trebuie pus neaparat o valoare de tipul data si timp exact la numberofperiod

    # Trebuie mecanisme de verificare!!! mai ales ca difera ultima chestie!! + sa blocheze de pe android sa scrii in mai multe o data, sau daca scrii
    # in mai multe o data sa le faca pe rand (daca am un buton de ok... ) sau pun ok la fiecare... la statistics merge doar una o data..

    # Defapt si la statistics si la value in android pot face o singura linie pentru completarea campurilor si fac o lista si pentru cele de la a, b, c, d ...

    # De ex sa arate ceva default pe font gri si in timp ce selectez optiunile sa se schimbe textul din casute si sa spuna "does not matter" si "format type: ....."

    # 3.2. Pentu toate de mai sus o sa urmeze si PeriodType : [days, weeks, months, years, specific_period] si dupa NumberOfPeriod : o valoare pentru cate zile ani etc in urma din ziua curenta / un interval de tipul data - data cumva


    # 4. Pentru statistics (aici fac doar pentru o perioada in urma fata de ziua curenta):

    # 4.1 Method :

    # a. mean_values 
    # b. biggest_values
    # c. smalles_values
    # d. all_values -> la all values e obligatoriu setat days si se vor lua toate valorile dintr-o zi, iar la number of period pun pt cate zile fac asta

    # 4.2.fiecare va avea PeriodType : [days, weeks, months, years] si NumberOfPeriod : o valoare pentru cate PeriodType; aici daca e days face media pe fiecare zi si pentru fiecare zi returneaza o valoare pt grafic. la fel si pt luna an etc... si number of period e numarul de puncte de pe graf 