import pyspark
import requests
import json
import os
from supabase import create_client, Client
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    IntegerType,
    StringType,
)
from pyspark.sql.functions import col, avg
from pyspark.sql import Row
from pyspark.sql import SQLContext
from datetime import datetime
from dateutil import tz
from pyspark.sql import functions as F

# Crea una sesión de Spark
spark = SparkSession.builder.appName("InventarioDashboard").getOrCreate()
# Crea un SQLContext a partir de la sesión de Spark
sqlContext = SQLContext(spark)

# Salida
output_api_url = "https://nsrprlygqaqgljpfggjh.supabase.co"
output_api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5zcnBybHlncWFxZ2xqcGZnZ2poIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDAxNzU2MjUsImV4cCI6MjAxNTc1MTYyNX0.JQUJ2i2mZlygBys5Gd5elAL_00TM_U2vJrXlIVuOtbk"
supabase_client_out: Client = create_client(output_api_url, output_api_key)


def actualizar_registro_todos(
    id_existing_record,
    countGateways,
    countAGateways,
    countSIM,
    countASIMs,
    countBundle,
    countABundle,
    json_result_clean,
    sim_data,
    gateway_data,
    id_existing_records,
    supabase_table,
):
    """
    Función para actualizar o insertar un registro en Supabase para la tabla "inventory_dashboards".

    Args:
        id_existing_record: valor del registro existente que se va a actualizar (si aún no existe en la tabla su valor es 0) (int)
        gateway_totals: cuantos registros hay del tipo Gateway (float),
        gateway_actives: cuantos Gateways estan activos (float),
        sims_total: Cuantos registros hay del tipo SIM (float),
        sims_actives: Cunatos SIM estan activas (float),
        bundles_total: Cuantos registros hay del tipo Bundle (float),
        bundles_active: Cuantos Bundles estab actuvia (float),
        json_result_clean: json con los registros por type y location (jsonb),
        sim_data: json con los registros de los SIMs (jsonb),
        gateway_data: json con los registros de los Gateways (jsonb)
        id_existing_records: lista de id records ya existentes
        supabase_table: nombre de la tabla de supabase donde se hace el CRUD de datos
    Returns:
        Null.
    """
    # Verifica si ya existe el registro con "id_existing_record" actual en Supabase
    if id_existing_record != 0:
        # Se realiza el UPDATE del registro
        responseUpdate = (
            supabase_client_out.table(supabase_table)
            .update(
                {
                    "gateway_totals": countGateways,
                    "gateway_actives": countAGateways,
                    "sims_total": countSIM,
                    "sims_actives": countASIMs,
                    "bundles_total": countBundle,
                    "bundles_active": countABundle,
                    "bundle_location": json_result_clean,
                    "sims_location": sim_data,
                    "gateway_location": gateway_data,
                }
            )
            .eq("id", id_existing_record)
            .execute()
        )
        # Verifica la respuesta exitosa y maneja errores si es necesario
        if responseUpdate.data:
            # Se quita el valor del "id" del registro, con la condición que exista en la lista "id_existing_records"
            if id_existing_record in id_existing_records:
                id_existing_records.remove(id_existing_record)
            print(f"Registro actualizado con id: {id_existing_record}")
        else:
            print(f"Error al actualizar registro con id: {id_existing_record}: {error}")
    else:
        # Se realiza el CREATE del registro
        responseCreate = (
            supabase_client_out.table(supabase_table)
            .insert(
                {
                    "gateway_totals": countGateways,
                    "gateway_actives": countAGateways,
                    "sims_total": countSIM,
                    "sims_actives": countASIMs,
                    "bundles_total": countBundle,
                    "bundles_active": countABundle,
                    "bundle_location": json_result_clean,
                    "sims_location": sim_data,
                    "gateway_location": gateway_data,
                }
            )
            .execute()
        )
        # Verifica la respuesta exitosa y maneja errores si es necesario
        if responseCreate.data:
            print(f"Registro creado con id: {responseCreate.data[0]['id']}")
        else:
            print(f"Error al crear registro: {error}")


# Función principal del Proceso
# if __name__ == "__main__":
def main():
    try:
        # Define el esquema personalizado
        schema = StructType(
            [
                StructField("inventory_product_id", IntegerType(), False),
                StructField("name", StringType(), False),
                StructField("type", StringType(), False),
                StructField("product_type_id", IntegerType(), False),
                StructField("location", StringType(), False),
                StructField("inventory_location_id", IntegerType(), False),
                StructField("status", StringType(), False),
                StructField("inventory_product_status_id", IntegerType(), False),
                # El valor puede ser Null (True)
                StructField("created_at", StringType(), True),
            ]
        )

        id_existing_records = []
        id_existing_record = 0

        # Define la tabla donde se va a hacer el CRUD de Datos
        supabase_table = "inventory_dashboards"
        # Entrada
        # API de Entrada de Supabase
        input_api_url = "https://nsrprlygqaqgljpfggjh.supabase.co/rest/v1/inventory_dashboards_view?select=*"
        # Clave de Entrada de Supabase
        input_api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5zcnBybHlncWFxZ2xqcGZnZ2poIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDAxNzU2MjUsImV4cCI6MjAxNTc1MTYyNX0.JQUJ2i2mZlygBys5Gd5elAL_00TM_U2vJrXlIVuOtbk"
        # Configura los Headers con la clave de autenticación
        input_headers = {"apikey": f"{input_api_key}"}
        # Realiza una solicitud GET para obtener datos de Entrada
        input_response = requests.get(input_api_url, headers=input_headers)
        # <<< EXTRACCIÓN >>>
        # Verifica si la solicitud de Entrada fue exitosa
        if input_response.status_code == 200:
            # Guarda la respuesta JSON como una lista de diccionarios de inventario
            responseJSON = input_response.json()
            # Convierte cada registro JSON en un objeto Row con el esquema definido
            # 'input_response.get("clave", None)' Intenta recuperar el valor con la clave de la primera posición, en caso de que no exista regresa "None"
            responseRow = [
                Row(
                    productID=response.get("inventory_product_id", None),
                    productName=response.get("name", None),
                    productType=response.get("type", None),
                    productTypeId=response.get("product_type_id", None),
                    productLocation=response.get("location", None),
                    productLocatuionID=response.get("inventory_location_id", None),
                    productStatus=response.get("status", None),
                    productStatusId=response.get("inventory_product_status_id", None),
                    productCreationDate=response.get("created_at", None),
                )
                for response in responseJSON
            ]
            # <<< TRANSFORMACIÓN >>>
            # Crea un DataFrame a partir de la lista de objetos Row y el esquema
            responseDF = sqlContext.createDataFrame(responseRow, schema=schema)
            randomDF = responseDF.sample(False, 0.5).limit(10)
            # Muestra 10 registros aleatorios del DataFrame
            randomDF.show()
            today_date = datetime.now(tz.tzutc())
            current_date = today_date.strftime("%Y-%m-%d")
            # Verifica si el DataFrame tiene contenido
            if responseDF.isEmpty() == False:
                # <<< CÁLCULOS >>>

                # Contar el número de registros donde la columna "productType" tiene el valor "gateway"
                countGateways = responseDF.filter(
                    (col("product_type_id") == 1)
                    & (F.date_format("created_at", "yyyy-MM-dd") == current_date)
                ).count()
                # Contar el nuemero de registros donde sean Gateway la columna "status" tiene el valor de Connected
                countAGateways = responseDF.filter(
                    (col("product_type_id") == 1)
                    & (col("inventory_product_status_id") == 6)
                    & (F.date_format("created_at", "yyyy-MM-dd") == current_date)
                ).count()

                # Contar el número de registros donde la columna "productType" contiene la palabra "SIM"
                countSIM = responseDF.filter(
                    (col("product_type_id") == 2)
                    & (F.date_format("created_at", "yyyy-MM-dd") == current_date)
                ).count()
                # Contar el nuemero de registros donde sean SIM la columna "status" tiene el valor de Connected
                countASIMs = responseDF.filter(
                    (col("product_type_id") == 2)
                    & (col("inventory_product_status_id") == 6)
                    & (F.date_format("created_at", "yyyy-MM-dd") == current_date)
                ).count()
                # Contar el número de registros donde la columna "productType" contiene la palabra "Bundle"
                countBundle = responseDF.filter(
                    (col("type") == "Bundle")
                    & (F.date_format("created_at", "yyyy-MM-dd") == current_date)
                ).count()
                # Contar el nuemero de registros donde sean Bundle la columna "status" tiene el valor de Connected
                countABundle = responseDF.filter(
                    (col("type") == "Bundle")
                    & (col("status") == "Connected")
                    & (F.date_format("created_at", "yyyy-MM-dd") == current_date)
                ).count()

                # Filtrar responseDF para la fecha actual
                responseDF_filtered = responseDF.filter(
                    (F.to_date("created_at") == F.current_date())
                )

                # Realizar la agrupación por ubicación y tipo en el DataFrame filtrado
                gatewayLocation = responseDF_filtered.groupBy("location", "type").agg(
                    F.count("*").alias("count_per_location_and_type")
                )

                # Convertir el DataFrame resultante a JSON y recopilar los resultados
                json_result = gatewayLocation.toJSON().collect()

                # Eliminar los caracteres de escape adicionales de cada cadena JSON
                json_result_clean = [json.loads(row) for row in json_result]

                # Separar los datos según el valor de "type"
                sim_data = [row for row in json_result_clean if row["type"] == "SIM"]
                gateway_data = [
                    row for row in json_result_clean if row["type"] == "Gateway"
                ]

                # print(countAGateways)
                # print("------------------")
                # print(countGateways)
                # print("------------------")
                # print(countSIM)
                # print("------------------")
                # print(countASIMs)
                # print("------------------")
                # print(countBundle)
                # print("------------------")
                # print(countABundle)
                # print("------------------")
                # print(sim_data)
                # print("------------------")
                # print(gateway_data)

                # <<< INSERCIÓN >>>
                # Actualiza, inserta o elimina datos en la tabla de Supabase
                # Obtener la fecha actual en el formato correcto
                fecha_actual_inicio = today_date.strftime("%Y-%m-%dT00:00:00.%f%z")
                fecha_actual_fin = today_date.strftime("%Y-%m-%dT23:59:59.%f%z")
                # Realiza una consulta SQL para recuperar los valores de "id" de los registros en Supabase
                queryStoredData = "id"
                print("-------------Consulta-----------------")
                responseStoredData = (
                    supabase_client_out.table(supabase_table)
                    .select(queryStoredData)
                    .gte("created_at", f"{fecha_actual_inicio}")
                    .lte("created_at", f"{fecha_actual_fin}")
                    .execute()
                )
                print("------Se Realizo la Consulta------")
                # Verifica si hubo un error en la consulta
                if responseStoredData.data:
                    # Almacena los valores de "id" en una lista
                    id_existing_records = [
                        record["id"] for record in responseStoredData.data
                    ]
                    print(
                        f"Lista recuperada de los id registrados existentes: {id_existing_records}"
                    )
                    id_existing_record = responseStoredData.data[0]["id"]
                else:
                    print(
                        f"No se pudieron recuperar valores {queryStoredData} de la tabla {supabase_table}"
                    )
                actualizar_registro_todos(
                    id_existing_record,
                    countGateways,
                    countAGateways,
                    countSIM,
                    countASIMs,
                    countBundle,
                    countABundle,
                    json_result_clean,
                    sim_data,
                    gateway_data,
                    id_existing_records,
                    supabase_table,
                )
                print(
                    f"Tamaño de Lista de registros a eliminar: {len(id_existing_records)}"
                )
                if len(id_existing_records) > 0:
                    for record in id_existing_records:
                        supabase_client_out.table(supabase_table).delete().eq(
                            "id", record
                        ).execute()  # Elimina registro
            else:
                print("El DataFrame no contiene datos.")
            spark.stop()
            return "Successfull Process", True
        else:
            resultado = "Fail to recover data from Supabase Client"
            return resultado, False
    except Exception as e:
        # Manejo de otras excepciones
        resultado = f"Error: {str(e)}"
        return resultado, False
