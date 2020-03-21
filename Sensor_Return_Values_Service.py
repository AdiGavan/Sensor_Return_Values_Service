from flask import Flask, request, jsonify

app = Flask(__name__)

# Single Value
def query_single_mean_value(sensorType, periodType, numberOfPeriod):
    # TODO interogare db
    # Test raspuns
    json_answer = jsonify({"SensorType" : sensorType, "Method" : "query_single_mean_value", "PeriodType" : periodType, "NumberOfPeriod" : numberOfPeriod})
    return json_answer

def query_single_smallest_value(sensorType, periodType, numberOfPeriod):
    # TODO interogare db
    # Test raspuns
    json_answer = jsonify({"SensorType" : sensorType, "Method" : "query_single_smallest_value", "PeriodType" : periodType, "NumberOfPeriod" : numberOfPeriod})
    return json_answer

def query_single_biggest_value(sensorType, periodType, numberOfPeriod):
    # TODO interogare db
    # Test raspuns
    json_answer = jsonify({"SensorType" : sensorType, "Method" : "query_single_biggest_value", "PeriodType" : periodType, "NumberOfPeriod" : numberOfPeriod})
    return json_answer

def query_single_oldest_value(sensorType, periodType, numberOfPeriod):
    # TODO interogare db
    # Test raspuns
    json_answer = jsonify({"SensorType" : sensorType, "Method" : "query_single_oldest_value", "PeriodType" : periodType, "NumberOfPeriod" : numberOfPeriod})
    return json_answer

def query_single_newest_value(sensorType, periodType, numberOfPeriod):
    # TODO interogare db
    # Test raspuns
    json_answer = jsonify({"SensorType" : sensorType, "Method" : "query_single_newest_value", "PeriodType" : periodType, "NumberOfPeriod" : numberOfPeriod})
    return json_answer

def query_single_specific_moment(sensorType, periodType, numberOfPeriod):
    date_and_time = numberOfPeriod.split(" ")
    time = date_and_time[1]
    date = date_and_time[0]
    
    # TODO interogare db
    # Test raspuns
    json_answer = jsonify({"SensorType" : sensorType, "Method" : "query_single_specific_moment", "PeriodType" : periodType, "NumberOfPeriod" : numberOfPeriod})
    return json_answer

# Function for querying the database and returning a single value
def query_single_values(sensorType, methodType, periodType, numberOfPeriod):
    if methodType == "mean_value":
        json_answer = query_single_mean_value(sensorType, periodType, numberOfPeriod)

    elif methodType == "smallest_value":
        json_answer = query_single_smallest_value(sensorType, periodType, numberOfPeriod)

    elif methodType == "biggest_value":
        json_answer = query_single_biggest_value(sensorType, periodType, numberOfPeriod)
    
    elif methodType == "oldest_value":
        json_answer = query_single_oldest_value(sensorType, periodType, numberOfPeriod)

    elif methodType == "newest_value":
        json_answer = query_single_newest_value(sensorType, periodType, numberOfPeriod)

    elif methodType == "specific_moment":
        json_answer = query_single_specific_moment(sensorType, periodType, numberOfPeriod)
    
    return json_answer
    
    
# Multiple values for statistics
def query_multiple_mean_value(sensorType, periodType, numberOfPeriod):
    # TODO interogare db
    # Test raspuns
    json_answer = jsonify({"SensorType" : sensorType, "Method" : "query_multiple_mean_value", "PeriodType" : periodType, "NumberOfPeriod" : numberOfPeriod})
    return json_answer

def query_multiple_smallest_value(sensorType, periodType, numberOfPeriod):
    # TODO interogare db
    # Test raspuns
    json_answer = jsonify({"SensorType" : sensorType, "Method" : "query_multiple_smallest_value", "PeriodType" : periodType, "NumberOfPeriod" : numberOfPeriod})
    return json_answer

def query_multiple_biggest_value(sensorType, periodType, numberOfPeriod):
    # TODO interogare db
    # Test raspuns
    json_answer = jsonify({"SensorType" : sensorType, "Method" : "query_multiple_biggest_value", "PeriodType" : periodType, "NumberOfPeriod" : numberOfPeriod})
    return json_answer

def query_multiple_all_values(sensorType, periodType, numberOfPeriod):
    # TODO interogare db
    # Test raspuns
    json_answer = jsonify({"SensorType" : sensorType, "Method" : "query_multiple_all_values", "PeriodType" : periodType, "NumberOfPeriod" : numberOfPeriod})
    return json_answer

# Function for querying the database and returning a multiple values for statistics
def query_multiple_values(sensorType, methodType, periodType, numberOfPeriod):
    if methodType == "mean_value":
        json_answer = query_multiple_mean_value(sensorType, periodType, numberOfPeriod)

    elif methodType == "smallest_value":
        json_answer = query_multiple_smallest_value(sensorType, periodType, numberOfPeriod)

    elif methodType == "biggest_value":
        json_answer =  query_multiple_biggest_value(sensorType, periodType, numberOfPeriod)

    elif methodType == "all_values":
        json_answer =  query_multiple_all_values(sensorType, periodType, numberOfPeriod)
    
    return json_answer

 
@app.route('/', methods=['GET'])
def take_data():

    jsonData = request.get_json()
    returnType = jsonData['ReturnType']
    sensorType = jsonData['SensorType']
    methodType = jsonData['Method']
    periodType = jsonData['PeriodType']
    numberOfPeriod = jsonData['NumberOfPeriod']
    
    # TODO conectare db

    # Tipul senzorului nu trebuie parsat pentru ca il dau functiei direct si in db o sa se faca interogarea doar pentru tipul respectiv de senzor
    if returnType == "single_value":
        json_answer = query_single_values(sensorType, methodType, periodType, numberOfPeriod)

    elif returnType == "multiple_values":
        json_answer = query_multiple_values(sensorType, methodType, periodType, numberOfPeriod)

    return json_answer

if __name__ == "__main__":
    app.run(host="0.0.0.0")



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