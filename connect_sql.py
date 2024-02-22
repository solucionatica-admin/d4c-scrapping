#Librerías para conexión a base de datos
import pyodbc
import pandas as pd
from tabulate import tabulate

#Variables de base de datos
server='100.25.35.43'
database='ProyectoCDC'
username = 'sa'
password = 'd4cAdmin'
tabla ='CONTENEDOR'

connection = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};'

def insert(df):
    list = df.values.tolist()
    try:
        conn = pyodbc.connect(connection)
        cursor = conn.cursor()

        # Realiza operaciones en la base de datos
        cursor.executemany(f"INSERT INTO {tabla} VALUES (?,?,?,?,?,?,?)",list)
        cursor.commit()
        
        # Cierra el cursor y la conexión
        cursor.close()
        conn.close()

    except pyodbc.Error as e:
        print("Error al conectar a SQL Server:", str(e))

def update(df):
    # Establece la conexión con la base de datos
    conn = pyodbc.connect(connection)
    cursor = conn.cursor()

    # Construye la consulta UPDATE dinámicamente
    update_query = f"UPDATE CONTENEDOR SET "

    for col in df.columns:
        if col != 'Número':
            update_query += f"{col} = '{df[col][0]}', "

    # Elimina la última coma y espacio innecesarios
    update_query = update_query[:-2]

    # Agrega la condición WHERE
    update_query += f" WHERE Número = '{df['Número'][0]}'"

    try:
        # Ejecuta la consulta
        cursor.execute(update_query)
        conn.commit()
        print("Actualización exitosa.")
    except Exception as e:
        print(f"Error durante la actualización: {str(e)}")
    finally:
        # Cierra la conexión
        cursor.close()
        conn.close()


def validate(df):
    # Conecta a SQL Server
    conn = pyodbc.connect(connection)
    print(type(df))
    print(tabulate(df, headers='keys', tablefmt='psql'))
    try:
        # Construye la consulta SQL parametrizada
        print(df.at[0,"Número"])
        numero = df.at[0,"Número"]
        query = f"SELECT COUNT(*) FROM CONTENEDOR WHERE Número = '{numero}'"
        print(query)
        print()
        cursor = conn.cursor()
        cursor.execute(query)
        row_count = cursor.fetchone()[0]
        
        # Cierra la conexión a la base de datos
        conn.close()
        if(row_count>0):
            #Existe alguna fila
            return 1
        else:
            #Solo si sale 0, lo va a insertar
            return 0
    
    except Exception as e:
        print(type(e).__name__)
        return -1
