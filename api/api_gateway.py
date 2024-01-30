from flask import Flask, request, jsonify, make_response
import os
from flask_cors import CORS
from datetime import datetime
from scripts import sum_total_indicators, indicators_month_yearly

app = Flask(__name__)
CORS(app)  # Habilita CORS para toda la aplicación

@app.route('/apigateway/update/sum_total_indicators/<int:document_id>',  methods=['GET', 'OPTIONS']) # Agregar método POST, DELETE, FETCH cuando se requiera
def sum_total_indicator(document_id):
    if request.method == 'OPTIONS':
        response = make_response()
        # Agregar headers correspondientes, dependiendo el método
        response.headers['Access-Control-Allow-Methods'] = 'GET,OPTIONS' # Agregar método POST, DELETE, FETCH cuando se requiera
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response
    
    if request.method == 'GET':
        print("Calling to Sum Total Indicators")
        result,bool = sum_total_indicators.main(document_id)
        # Getting date today
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            response = make_response(jsonify({'message': f'Successfull process sum_total_indicators on {fecha_hoy}'}))
        else:
            response = make_response(jsonify({'message': f'Failed process sum_total_indicators on {fecha_hoy}, details: {result}'}))
        return response

@app.route('/apigateway/update/indicators_month_yearly/<int:document_id>',  methods=['GET', 'OPTIONS']) # Agregar método POST, DELETE, FETCH cuando se requiera
def sum_total_indicator(document_id):
    if request.method == 'OPTIONS':
        response = make_response()
        # Agregar headers correspondientes, dependiendo el método
        response.headers['Access-Control-Allow-Methods'] = 'GET,OPTIONS' # Agregar método POST, DELETE, FETCH cuando se requiera
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response
    
    if request.method == 'GET':
        print("Calling to Sum Total Indicators")
        result,bool = indicators_month_yearly.main(document_id)
        # Getting date today
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            response = make_response(jsonify({'message': f'Successfull process indicators_month_yearly on {fecha_hoy}'}))
        else:
            response = make_response(jsonify({'message': f'Failed process indicators_month_yearly on {fecha_hoy}, details: {result}'}))
        return response

@app.route('/apigateway/update/indicators_quarter_yearly/<int:document_id>',  methods=['GET', 'OPTIONS']) # Agregar método POST, DELETE, FETCH cuando se requiera
def sum_total_indicator(document_id):
    if request.method == 'OPTIONS':
        response = make_response()
        # Agregar headers correspondientes, dependiendo el método
        response.headers['Access-Control-Allow-Methods'] = 'GET,OPTIONS' # Agregar método POST, DELETE, FETCH cuando se requiera
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response
    
    if request.method == 'GET':
        print("Calling to Sum Total Indicators")
        result,bool = sum_total_indicators.main(document_id)
        # Getting date today
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            response = make_response(jsonify({'message': f'Successfull process sum_total_indicators on {fecha_hoy}'}))
        else:
            response = make_response(jsonify({'message': f'Failed process sum_total_indicators on {fecha_hoy}, details: {result}'}))
        return response

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8089))
    app.run(debug=True, host='0.0.0.0', port=port)