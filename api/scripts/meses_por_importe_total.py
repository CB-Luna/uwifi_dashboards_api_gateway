import pyspark
import csv
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from supabase import create_client, Client
from pyspark.sql.types import StructType, StructField, IntegerType, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, avg, sum
from pyspark.sql import Row
from pyspark.sql import SQLContext
from datetime import datetime
from dateutil import tz

 # Crea una sesión de Spark
spark = SparkSession.builder.appName("MesesPorImporteTotal").getOrCreate()
# Configuración para usar el antiguo analizador de fechas
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# Crea un SQLContext a partir de la sesión de Spark
sqlContext = SQLContext(spark)
# Define el esquema personalizado
schema = StructType([
    StructField("rev_parup", IntegerType(), False),
    StructField("super_chain_name", StringType(), False),
    StructField("property_name", StringType(), False),
    StructField("street_address", StringType(), False),
    StructField("hotel_country", StringType(), False),
    StructField("region", StringType(), False),
    StructField("number_rooms", IntegerType(), False),
    StructField("room_nights", IntegerType(), False),
    StructField("supplemental_charges", FloatType(), False),
    StructField("total_hotel_cost", FloatType(), False),
    StructField("total_hotel_charges", FloatType(), False),
    StructField("booking_date", StringType(), False)
])

# URL del servidor que contiene el archivo CSV remoto
servidor_remoto_url = "https://lagiinfnnpqlsydoriqt.supabase.co/storage/v1/object/public/hotels/source/"
# Nombre del archivo CSV en el servidor remoto
archivo_csv_remoto = "Historical-Data-Hotels.csv?t=2023-12-22T16%3A38%3A24.734Z"
# URL completa del archivo CSV en el servidor remoto
url_csv_remoto = servidor_remoto_url + archivo_csv_remoto

# Registros existentes
existing_records = []

# Define la tabla donde se va a hacer el CRUD de Datos
supabase_table = "month_import"
# Salida
output_api_url = "https://lagiinfnnpqlsydoriqt.supabase.co"
output_api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxhZ2lpbmZubnBxbHN5ZG9yaXF0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTc3NDM1MzEsImV4cCI6MjAxMzMxOTUzMX0.3nZOm4gzHdM27FqxUUOSzYl5eBuHQXzCNPof3Fk83po"
supabase_client_out: Client = create_client(output_api_url, output_api_key)

# Función para actualizar un registro en Supabase
def actualizar_registro(row):
    # Define el valor que permite verificar si existe el registro (por ejemplo, el valor de la columna "month_year")
    month_year = row["month_year"]
    # Define los valores que se van a INSERTAR o ACTUALIZAR
    max_property_name = row["max_property_name"]
    min_property_name = row["min_property_name"]
    sum_total_hotel_cost = row["sum_total_hotel_cost"]
    sum_total_hotel_charges = row["sum_total_hotel_charges"]
    sum_supplemental_charges = row["sum_supplemental_charges"]
    # Realiza una consulta para verificar si el registro existe
    # En este ejemplo, estamos verificando si existe un registro con un valor específico en la columna "month_year"
    condition = month_year
    queryExistingRow = "month_year"
    responseExistingRow = supabase_client_out.table(supabase_table).select(queryExistingRow).eq("month_year", condition).execute()
    # Verifica si ya existe el registro de la iteración actual en Supabase
    if responseExistingRow.data:
        # Se realiza el UPDATE del registro
        responseUpdate = supabase_client_out.table(supabase_table).update({"max_property_name": max_property_name, "min_property_name": min_property_name, "sum_total_hotel_cost": sum_total_hotel_cost, "sum_total_hotel_charges": sum_total_hotel_charges, "sum_supplemental_charges": sum_supplemental_charges}).eq("month_year", responseExistingRow.data[0]["month_year"]).execute()
        # Verifica la respuesta exitosa y maneja errores si es necesario
        if responseUpdate.data:
            print(f"Registro actualizado: {month_year}")
        else:
            print(f"Error al actualizar registro {month_year}: {error}")
    else:
        # Se realiza el CREATE del registro
        responseCreate = supabase_client_out.table(supabase_table).insert({"month_year": month_year, "max_property_name": max_property_name, "min_property_name": min_property_name, "sum_total_hotel_cost": sum_total_hotel_cost, "sum_total_hotel_charges": sum_total_hotel_charges, "sum_supplemental_charges": sum_supplemental_charges}).execute()
        # Verifica la respuesta exitosa y maneja errores si es necesario
        if responseCreate.data:
            print(f"Registro creado: {month_year}")
        else:
            print(f"Error al crear registro {month_year}: {error}")

# Función principal del Proceso
if __name__ == "__main__":
    try:
        print("Inicia Proceso de recuperación")
        # Realiza una solicitud HTTP GET para obtener el contenido del archivo CSV
        response = requests.get(url_csv_remoto)
        # Verifica si la solicitud se completó exitosamente
        if response.status_code == 200:
            print("Sí se pudo recuperar el archivo CSV")
            # Decodifica el contenido de la respuesta como texto CSV
            csv_content = response.text
            # Analiza el contenido CSV
            csv_reader = csv.reader(csv_content.splitlines())
            # Salta las primeras tres filas
            next(csv_reader)
            next(csv_reader)
            next(csv_reader)
            rows = list(csv_reader)
            # Convierte las filas a objetos Row con el esquema especificado
            data_rows = [Row(
                rev_parup=int(row[0]) if row[0] else 0,  # Convierte a Integer, maneja el caso en que sea None o vacío
                super_chain_name=row[4] if row[4] else "NO_SUPER_CHAIN_NAME",
                property_name=row[7] if row[7] else "NO_PROPERTY_NAME",
                street_address=row[8] if row[8] else "NO_STREET_ADDRESS",
                hotel_country=row[23] if row[23] else "NO_HOTEL_COUNTRY",
                region=row[24] if row[24] else "NO_REGION",
                number_rooms=int(row[31]) if row[31] else 0,  # Convierte a Integer, maneja el caso en que sea None o vacío
                room_nights=int(row[32]) if row[32] else 0,  # Convierte a Integer, maneja el caso en que sea None o vacío
                supplemental_charges=float(row[34]) if row[34] else 0.0,  # Convierte a Float, maneja el caso en que sea None o vacío
                total_hotel_cost=float(row[33]) if row[33] else 0.0,  # Convierte a Float, maneja el caso en que sea None o vacío
                total_hotel_charges=float(row[35]) if row[35] else 0.0,  # Convierte a Float, maneja el caso en que sea None o vacío
                booking_date=row[2] if row[2] else "NO_BOOKING_DATE",
                ) for row in rows]
            # Crea un DataFrame a partir de las filas y el esquema
            df = spark.createDataFrame(data_rows, schema=schema)
            df.show(4)
            # Convierte la columna de fecha al formato correcto
            df = df.withColumn("booking_date", F.to_date(df["booking_date"], "M/dd/yyyy"))

            # Extrae mes y año de la columna booking_date
            df = df.withColumn("month_year", F.date_format("booking_date", "yyyy-MM"))

            # Agrupa por mes y año y realiza los cálculos
            result_df = df.groupBy("month_year").agg(
                F.sum("supplemental_charges").alias("sum_supplemental_charges"),
                F.sum("total_hotel_cost").alias("sum_total_hotel_cost"),
                F.sum("total_hotel_charges").alias("sum_total_hotel_charges"),
                F.max("property_name").alias("max_property_name"),
                F.min("property_name").alias("min_property_name")
            )
            # Filtrar las filas donde month_year no es NULL
            result_df = result_df.filter(F.col("month_year").isNotNull())
            # Agrupa por 'month_year' y calcula las sumas
            # Muestra el resultado
            result_df.show(10)
            print(f"Filas: {result_df.count()}")
            # Realiza una consulta SQL para recuperar los valores de "month_year" de los registros en Supabase
            queryStoredData = "month_year"
            responseStoredData = supabase_client_out.table(supabase_table).select(queryStoredData).execute()
            # Verifica si hubo un error en la consulta
            if responseStoredData.data:
                # Almacena los valores de "month_year" en una lista
                existing_records = [record["month_year"] for record in responseStoredData.data]
                print(f"Lista recuperada de los month_year registrados existentes: {existing_records}")
            else:
                print(f"No se pudieron recuperar valores {queryStoredData} de la tabla {supabase_table}")
            # Itera a través de las filas del DataFrame y aplica la función de actualización
            rows2 = result_df.rdd.map(lambda row: row.asDict()).collect() # NO HACER collet()
            for row in rows2:
                actualizar_registro(row)
                if row["month_year"] in existing_records:
                    existing_records.remove(row["month_year"])
            print(f"Tamaño de Lista de registros a eliminar: {len(existing_records)}")
            if len(existing_records) > 0:
                for record in existing_records:
                    supabase_client_out.table(supabase_table).delete().eq("month_year", record).execute() # Elimina registro
            spark.stop()
        else:
            print(f"No se pudo obtener el archivo CSV del servidor remoto. Código de estado: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la solicitud HTTP: {e}")
    except Exception as e:
        print(f"Ocurrió un error: {e}")