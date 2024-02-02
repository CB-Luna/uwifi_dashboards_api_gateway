from flask import Flask, request, jsonify, make_response
import os
from flask_cors import CORS
from datetime import datetime
from scripts import inventario_dashboards

app = Flask(__name__)
CORS(app)  # Habilita CORS para toda la aplicación


@app.route(
    "/apigateway/update/inventario_dashboards",
    methods=["GET", "OPTIONS"],
)  # Agregar método POST, DELETE, FETCH cuando se requiera
def inventario_dashboards_function():
    if request.method == "OPTIONS":
        response = make_response()
        # Agregar headers correspondientes, dependiendo el método
        response.headers[
            "Access-Control-Allow-Methods"
        ] = "GET,OPTIONS"  # Agregar método POST, DELETE, FETCH cuando se requiera
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        response.headers["Access-Control-Allow-Origin"] = "*"
        return response

    if request.method == "GET":
        print("Calling to InventorioDashboards")
        result, bool = inventario_dashboards.main()
        # Getting date today
        fecha_hoy = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if bool:
            response = make_response(
                jsonify(
                    {
                        "message": f"Successfull process inventario_dashboards_function on {fecha_hoy}"
                    }
                )
            )
        else:
            response = make_response(
                jsonify(
                    {
                        "message": f"Failed process inventario_dashboards_function on {fecha_hoy}, details: {result}"
                    }
                )
            )
        return response

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8088))
    app.run(debug=True, host="0.0.0.0", port=port)
