import pyspark
import csv
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from supabase import create_client, Client
from pyspark.sql.types import StructType, StructField, IntegerType, IntegerType, StringType, FloatType
from pyspark.sql import Row
from pyspark.sql import SQLContext
from io import BytesIO
from io import StringIO

# Crea una sesión de Spark
spark = SparkSession.builder.appName("SumTotalIndicators").getOrCreate()
# Crea un SQLContext a partir de la sesión de Spark
sqlContext = SQLContext(spark)

# Supabase Client
output_api_url = "https://qikvwtvspqaenqmjpkti.supabase.co"
output_api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFpa3Z3dHZzcHFhZW5xbWpwa3RpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDEzMTk3OTUsImV4cCI6MjAxNjg5NTc5NX0.t78dmEgGxu6pW8aDfw84Pv0BklG3jsG_UGl7mkSCx4M"
supabase_client: Client = create_client(output_api_url, output_api_key)

def actualizar_registro_todos(id_existing_record, id_document, sum_supplemental_charges, sum_total_hotel_cost, sum_total_hotel_charges, sum_reference_rate, sum_reference_cost, sum_lowest_rate, sum_low_cost, sum_rate_in_local_currency, id_existing_records, supabase_table):
    """
    Función para actualizar o insertar un registro en Supabase para la tabla "indicators_total_hotels".

    Args:
        id_existing_record: valor del registro existente que se va a actualizar (si aún no existe en la tabla su valor es 0) (int) 
        id_document: id el documento existente que se va a actualizar o crear y se define en la tabla como "id_document_fk" (int) 
        sum_supplemental_charges: valor de suma de supplemental charges (float).
        sum_total_hotel_cost: valor de suma de total hotel cost (float).
        sum_total_hotel_charges: valor de suma de total hotel charges (float).
        sum_reference_rate: valor de suma de reference rate (float).
        sum_reference_cost: valor de suma de reference cost (float).
        sum_lowest_rate: valor de suma de lowest rate (float).
        sum_low_cost: valor de suma de total low cost (float).
        sum_rate_in_local_currency: valor de suma de rate in local currency (float).
        id_existing_records: lista de id records ya existentes
        supabase_table: nombre de la tabla de supabase donde se hace el CRUD de datos
    Returns:
        Null.
    """
    # Se reasigna el id del documento ("id_document_fk") a la condición para insertar o actualizar el registro en la tabla
    condition = id_document
    # Verifica si ya existe el registro con "id_existing_record" actual en la tabla de Supabase
    if id_existing_record != 0:
        # Se realiza el UPDATE del registro
        responseUpdate = supabase_client.table(supabase_table).update({"sum_total_a":sum_supplemental_charges, "sum_total_b":sum_total_hotel_cost, "sum_total_c":sum_total_hotel_charges, "sum_total_d":sum_reference_rate, "sum_total_e":sum_reference_cost, "sum_total_f":sum_lowest_rate, "sum_total_g":sum_low_cost, "sum_total_h":sum_rate_in_local_currency}).eq("id", id_existing_record).execute()
        # Verifica la respuesta exitosa y maneja errores si es necesario
        if responseUpdate.data:
            # Se quita el valor del "id" del registro, con la condición que exista en la lista "id_existing_records"
            if id_existing_record in id_existing_records:
                id_existing_records.remove(id_existing_record)
            print(f"Registro actualizado: {condition}")
        else:
            print(f"Error al actualizar registro {condition}: {error}")
    else:
        # Se realiza el CREATE del registro
        responseCreate = supabase_client.table(supabase_table).insert({"sum_total_a":sum_supplemental_charges, "sum_total_b":sum_total_hotel_cost, "sum_total_c":sum_total_hotel_charges, "sum_total_d":sum_reference_rate, "sum_total_e":sum_reference_cost, "sum_total_f":sum_lowest_rate, "sum_total_g":sum_low_cost, "sum_total_h":sum_rate_in_local_currency, "id_analisis": condition}).execute()
        # Verifica la respuesta exitosa y maneja errores si es necesario
        if responseCreate.data:
            print(f"Registro creado: {condition}")
        else:
            print(f"Error al crear registro {condition}: {error}")

# # Función principal del Proceso
# if __name__ == "__main__":
# Función principal del Proceso
def main(document_id: int):
    try:
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
            StructField("reference_rate", FloatType(), False),
            StructField("reference_cost", FloatType(), False),
            StructField("lowest_rate", FloatType(), False),
            StructField("low_cost", FloatType(), False),
            StructField("rate_in_local_currency", FloatType(), False),
            StructField("booking_date", StringType(), False)
        ])
        # Registros existentes
        id_existing_records = []
        id_existing_record = 0
        document_name = ""
        url_xlsx_remoto = ""
        # Define la tabla donde se va a hacer el CRUD de Datos
        supabase_table = "indicators_month_yearly"
        
        # Realiza una consulta SQL para recuperar el nombre del documento 
        querySourceDocument = "document"
        supabase_table_source = "documents"
        supabase_storage_source = "documents"
        responseSourceDocument = supabase_client.table(supabase_table_source).select(querySourceDocument).eq("id", document_id).execute()
        # Verifica si hubo un error en la consulta
        if responseSourceDocument.data:
            document_name = responseSourceDocument.data[0][querySourceDocument]
            print(f"Se recupera nombre de fuente existente: {document_name}")
            url_xlsx_remoto = supabase_client.storage.from_(supabase_storage_source).get_public_url(document_name)
            print(f"Se recupera url de fuente existente: {url_xlsx_remoto}")
            # Realiza una solicitud HTTP GET para obtener el contenido del archivo XLSX
            response = requests.get(url_xlsx_remoto)
            # Verifica si la solicitud se completó exitosamente
            if response.status_code == 200:
                print("Sí se pudo recuperar el archivo XLSX")
                # Lee el contenido del archivo .xlsx en un DataFrame de pandas
                df_pandas = pd.read_excel(BytesIO(response.content))
                # Crear un objeto StringIO para simular un archivo
                csv_data = StringIO()
                df_pandas.to_csv(csv_data, index=False)
                # Obtener un lector de CSV a partir de StringIO
                csv_reader = csv.reader(csv_data.getvalue().splitlines())
                # Saltar las primeras tres líneas
                for _ in range(3):
                    next(csv_reader)
                # Ahora puedes usar el lector de CSV desde aquí
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
                    reference_rate=float(row[36]) if row[36] else 0.0,  # Convierte a Float, maneja el caso en que sea None o vacío
                    reference_cost=float(row[37]) if row[37] else 0.0,  # Convierte a Float, maneja el caso en que sea None o vacío
                    lowest_rate=float(row[40]) if row[40] else 0.0,  # Convierte a Float, maneja el caso en que sea None o vacío
                    low_cost=float(row[41]) if row[41] else 0.0,  # Convierte a Float, maneja el caso en que sea None o vacío
                    rate_in_local_currency=float(row[47]) if row[47] else 0.0,  # Convierte a Float, maneja el caso en que sea None o vacío
                    booking_date=row[2] if row[2] else "NO_BOOKING_DATE",
                    ) for row in rows]
                # Crea un DataFrame a partir de las filas y el esquema
                df = spark.createDataFrame(data_rows, schema=schema)
                df.show(4)
                # Convierte la columna de fecha al formato correcto
                df = df.withColumn("booking_date", F.to_date(df["booking_date"], "M/dd/yyyy"))
                # Extrae mes y año de la columna booking_date
                df = df.withColumn("month_year", F.date_format("booking_date", "yyyy-MM"))
                # Selecciona las columnas que deseas sumar
                columnas_a_sumar = [
                    'supplemental_charges',
                    'total_hotel_cost',
                    'total_hotel_charges',
                    'reference_rate',
                    'reference_cost',
                    'lowest_rate',
                    'low_cost',
                    'rate_in_local_currency'
                ]
                # Crea un nuevo DataFrame con las sumas
                result_df = df.agg(*[F.round(F.sum(col), 2).alias(f'sum_{col}') for col in columnas_a_sumar])
                if result_df.isEmpty():
                    print("El DataFrame está vacío.")
                else:
                    print("El DataFrame tiene registros.")
                    # <<< INSERCIÓN >>>
                    # Actualiza, inserta o elimina datos en la tabla de Supabase
                    # Realiza una consulta SQL para recuperar los valores de "id" de los registros en Supabase
                    queryStoredData = "id"
                    responseStoredData = supabase_client.table(supabase_table).select(queryStoredData).eq("id_analisis", document_id).execute()
                    # Verifica si hubo un error en la consulta
                    if responseStoredData.data:
                        # Almacena los valores de "id" en una lista
                        id_existing_records = [record["id"] for record in responseStoredData.data]
                        print(f"Lista recuperada de los id registrados existentes: {id_existing_records}")
                        id_existing_record = responseStoredData.data[0]["id"]
                    else:
                        print(f"No se pudieron recuperar valores {queryStoredData} de la tabla {supabase_table}")
                    # Recupera la primera fila del DataFrame como objeto Row
                    row = result_df.collect()[0]
                    # Accede a los valores de las columnas específicas por nombre
                    sum_supplemental_charges = row["sum_supplemental_charges"]
                    sum_total_hotel_cost = row["sum_total_hotel_cost"]
                    sum_total_hotel_charges = row["sum_total_hotel_charges"]
                    sum_reference_rate = row["sum_reference_rate"]
                    sum_reference_cost = row["sum_reference_cost"]
                    sum_lowest_rate = row["sum_lowest_rate"]
                    sum_low_cost = row["sum_low_cost"]
                    sum_rate_in_local_currency = row["sum_rate_in_local_currency"]
                    # Imprime los valores recuperados
                    print("sum_supplemental_charges:", sum_supplemental_charges)
                    print("sum_total_hotel_cost:", sum_total_hotel_cost)
                    print("sum_total_hotel_charges:", sum_total_hotel_charges)
                    print("sum_reference_rate:", sum_reference_rate)
                    print("sum_reference_cost:", sum_reference_cost)
                    print("sum_lowest_rate:", sum_lowest_rate)
                    print("sum_low_cost:", sum_low_cost)
                    print("sum_rate_in_local_currency:", sum_rate_in_local_currency)
                    actualizar_registro_todos(id_existing_record, document_id, sum_supplemental_charges, sum_total_hotel_cost, sum_total_hotel_charges, sum_reference_rate, sum_reference_cost, sum_lowest_rate, sum_low_cost, sum_rate_in_local_currency, id_existing_records, supabase_table)
                    print(f"Tamaño de Lista de registros a eliminar: {len(id_existing_records)}")
                    if len(id_existing_records) > 0:
                        for record in id_existing_records:
                            supabase_client.table(supabase_table).delete().eq("id", record).execute() # Elimina registro
            else:
                resultado = f"Fail to getting XLSX File from Remote Server. Code Fail Status: {response.status_code}"
                return resultado, False
        else:
            resultado = f"Fail to recover {querySourceDocument} from table {supabase_table}"
            return resultado, False
        # spark.stop()
        return "Successfull Process", True
        
    except requests.exceptions.RequestException as e:
        # Manejo de otras excepciones
        resultado = f"Error al realizar la solicitud HTTP: {str(e)}"
        return resultado, False
    except Exception as e:
        # Manejo de otras excepciones
        resultado = f"Error: {str(e)}"
        return resultado, False