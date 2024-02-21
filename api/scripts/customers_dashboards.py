import pyspark
import requests
import json
import os
from supabase import create_client, Client
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.functions import col, avg, collect_list, count, to_date
from pyspark.sql import Row
from pyspark.sql import SQLContext
from datetime import datetime
from dateutil import tz
from pyspark.sql import functions as F

# Crea una sesión de Spark
spark = SparkSession.builder.appName("CustomerDashboards").getOrCreate()
# Crea un SQLContext a partir de la sesión de Spark
sqlContext = SQLContext(spark)

# Salida
output_api_url = "https://nsrprlygqaqgljpfggjh.supabase.co"
output_api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5zcnBybHlncWFxZ2xqcGZnZ2poIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDAxNzU2MjUsImV4cCI6MjAxNTc1MTYyNX0.JQUJ2i2mZlygBys5Gd5elAL_00TM_U2vJrXlIVuOtbk"
supabase_client_out: Client = create_client(output_api_url, output_api_key)


def actualizar_registro_todos(
    id_existing_record,
    countCustomers,
    countActive,
    countLead,
    json_result_clean,
    id_existing_records,
    supabase_table,
):
    """
    Función para actualizar o insertar un registro en Supabase para la tabla "customers_dashboards".

    Args:
        id_existing_record: valor del registro existente que se va a actualizar (si aún no existe en la tabla su valor es 0) (int)
        orders_totals: cuantas orders hay (float),
        countCustomers: Cuantos customers hay el dia de hoy (float),
        countActive: Cuantos customers estan activos (float),
        countLead: Cuantos customers son del tipo Lead (float),
        json_result_clean: json de los customers por tipo y sus ids,
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
                    "customers_totals": countCustomers,
                    "active_totals": countActive,
                    "lead_totals": countLead,
                    "new_customers_id": json_result_clean,
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
                    "customers_totals": countCustomers,
                    "active_totals": countActive,
                    "lead_totals": countLead,
                    "new_customers_id": json_result_clean,
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
#if __name__ == "__main__":
def main():
    try:
        # Define el esquema personalizado
        schema = StructType(
            [
                StructField("customer_id", IntegerType(), False),
                StructField("created_at", StringType(), False),
                StructField("first_name", StringType(), False),
                StructField("last_name", StringType(), False),
                StructField("status", StringType(), False),
            ]
        )

        id_existing_records = []
        id_existing_record = 0

        # Define la tabla donde se va a hacer el CRUD de Datos
        supabase_table = "customer_dashboards"
        # Entrada
        # API de Entrada de Supabase
        input_api_url = "https://nsrprlygqaqgljpfggjh.supabase.co/rest/v1/customer?select=customer_id,created_at,first_name,last_name,status"
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
                    customerID=response.get("customer_id", None),
                    customerCreatedAt=response.get("created_at", None),
                    customerFirstName=response.get("first_name", None),
                    customerLastName=response.get("last_name", None),
                    customerStatus=response.get("status", None),
                )
                for response in responseJSON
            ]
            # <<< TRANSFORMACIÓN >>>
            # Crea un DataFrame a partir de la lista de objetos Row y el esquema
            responseDF = sqlContext.createDataFrame(responseRow, schema=schema)
            # Convierte la columna 'created_at' al formato de fecha (YYYY-MM-DD)
            responseDF = responseDF.withColumn("created_at", to_date("created_at"))
            randomDF = responseDF.sample(False, 0.5).limit(10)
            # Muestra 10 registros aleatorios del DataFrame
            randomDF.show()
            today_date = datetime.now(tz.tzutc())
            current_date = today_date.strftime("%Y-%m-%d")
            # Verifica si el DataFrame tiene contenido
            if responseDF.isEmpty() == False:
                # <<< CÁLCULOS >>>

                # Contar los registros por la fecha de creación del pedido igual a la fecha actual
                countCustomers = responseDF.count()

                # Contar el nuemero de registros donde sean Active
                countActive = responseDF.filter(
                    # (col("order_creation").cast("date") == current_date)
                    (col("status") == "Active")
                ).count()
                # Contar el nuemero de registros donde sean order_type=2 aka Testing Router
                countLead = responseDF.filter(
                    # (col("order_creation").cast("date") == current_date)
                    (col("status") == "Lead")
                ).count()
                # # Filtrar los datos por la fecha actual
                # filtered_data = responseDF.filter(
                #     F.to_date("order_creation") == F.lit(current_date)
                # )
                result = responseDF.groupBy("status", "created_at").agg(
                    count("customer_id").alias("count"),
                    collect_list("customer_id").alias("customer_ids"),
                )

                # result = (
                #     responseDF.groupBy("status","created_at")
                #     .agg({"customer_id": "count", "customer_id": "collect_list"})
                #     .withColumnRenamed("count(customer_id)", "count")
                #     .withColumnRenamed("collect_list(customer_id)", "customer_ids")
                # )
                # Convertir el DataFrame resultante a JSON y recopilar los resultados
                json_result = result.toJSON().collect()

                # Eliminar los caracteres de escape adicionales de cada cadena JSON
                json_result_clean = [json.loads(row) for row in json_result]

                # print("------count customers------------")
                # print(countCustomers)
                # print("------count Active------------")
                # print(countActive)
                # print("-------count Lead-----------")
                # print(countLead)
                # print("-------json-----------")
                # print(json_result_clean)

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
                    countCustomers,
                    countActive,
                    countLead,
                    json_result_clean,
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
