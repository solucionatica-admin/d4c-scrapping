#LIBRERIAS PARA WEB SCRAPING
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

#CONEXIÓN CON SQL
import connect_sql as c

#CONEXIÓN CON NEO4J
from neo4j import GraphDatabase

#LIBRERÍAS OTRAS
import time as time
from datetime import datetime,timedelta
import pandas as pd
from io import StringIO
from tabulate import tabulate

import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

#CREDENCIALES NEO4J
URI = "neo4j+s://f9bbdf2b.databases.neo4j.io"
user = "neo4j"
password = "qp4jBK8dg6eabGurxSzjqzDPcAaNxy9SONFHjjbW_WQ"
AUTH = (user,password)

firefox_location = r'C:\Program Files\Mozilla Firefox\firefox.exe' #CAMBIAR POR LA RUTA DEL EJECUTABLE
driver_location = r'C:\Users\Brillitt\Desktop\Solucionática\Proyecto_CDC\SUNAT_WS\geckodriver.exe' #CAMBIAR POR LA RUTA DEL DRIVER
ruta_puertos = "puertos.csv" #CAMBIAR POR LA RUTA EN AMAZON S3

def insertNodev2(df, node_name):
    # Define una función para crear un nuevo nodo
    def crear_nodo(tx, atributos):
        query = f"CREATE (n:`{node_name}`) SET n = $atributos RETURN ID(n) as node_id"
        print(query)
        result = tx.run(query, atributos=atributos)
        return result.single()["node_id"]

    # Lista para almacenar los IDs generados
    node_ids = []

    # Establece una conexión a la base de datos Neo4j
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            for index, row in df.iterrows():
                atributos = dict(row)  # Todos los atributos de la fila
                node_id = session.write_transaction(crear_nodo, atributos)
                node_ids.append(node_id)

    return node_ids

def insertNode(df, node_name, key_col):
    # Define una función para verificar si un nodo ya existe
    def nodo_existe(tx, key_col_value):
        query = f"MATCH (n:`{node_name}`) WHERE n.`{key_col}` = $key_col_value RETURN n"
        print(query)
        result = tx.run(query, key_col_value=key_col_value)
        return result.single() is not None

    # Define una función para crear un nuevo nodo
    def crear_nodo(tx, atributos):
        query = f"CREATE (n:`{node_name}` $atributos)"
        print(query)
        tx.run(query, atributos=atributos)

    # Establece una conexión a la base de datos Neo4j
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            for index, row in df.iterrows():
              key_col_value = row[key_col]
              otros_atributos = dict(row)  # Todos los atributos de la fila

              # Verifica si el nodo ya existe
              if not session.read_transaction(nodo_existe, key_col_value):
                  # Si el nodo no existe, créalo
                  session.write_transaction(crear_nodo, otros_atributos)

def relateNodes(label,keyword_1,keyword_2):
  
  driver = GraphDatabase.driver(URI, auth=(user, password))

  if(label == "Bill of Lading"):
     query = ("MATCH (n:`Bill of Lading` {`Manifiesto+Detalle`:$value1}), (m:Bultos WHERE ID(m) = $value2) CREATE (n)-[:CONTIENE]->(m)")
  elif(label == "Bultos"):
     query = ("MATCH (n:Bultos WHERE ID(n) = $value1), (m:Contenedor {Número: $value2}) CREATE (n)-[:CONTIENE]->(m)")
  elif(label == "Manifiesto de carga"):
    query = ("MATCH (n:`Manifiesto de carga` {Manifiesto: $value1}), (m:`Bill of Lading` {`Manifiesto+Detalle`: $value2}) CREATE (n)-[:CONTIENE]->(m)")

  with driver.session() as session:
     session.write_transaction(create_relationship_transaction, query, keyword_1, keyword_2)
  driver.close()

def create_relationship_transaction(tx, query, value1, value2):
    tx.run(query, value1=value1, value2=value2) 
   

def getDate():
    #Hace 15 días
    start_day = (datetime.today() - timedelta(days=15)).strftime('%d/%m/%Y')
    print(start_day)
    return start_day

def accessDriver(link,driver_location,firefox_location):
   #Acceso al driver
   options = webdriver.FirefoxOptions()
   options.binary_location = firefox_location
   options.add_argument("--headless")
   options.add_argument('--log-level=3')
   driver = webdriver.Firefox(executable_path=driver_location,options=options)
   driver.get(link)
   return driver

def enterDates(driver):
    date = getDate()

    #Limpiar casillas de rango de fechas
    driver.find_element(By.XPATH,'/html[1]/body[1]/table[2]/tbody[1]/tr[2]/td[1]/div[1]/center[1]/table[1]/tbody[1]/tr[4]/td[2]/p[1]/font[1]/input[1]').clear()
    driver.find_element(By.XPATH,'/html[1]/body[1]/table[2]/tbody[1]/tr[2]/td[1]/div[1]/center[1]/table[1]/tbody[1]/tr[4]/td[2]/p[1]/font[2]/input[1]').clear()
    
    time.sleep(0.5)

    #Insertar rango de fechas en casillas
    WebDriverWait(driver, 3)\
      .until(EC.element_to_be_clickable((By.XPATH,'/html[1]/body[1]/table[2]/tbody[1]/tr[2]/td[1]/div[1]/center[1]/table[1]/tbody[1]/tr[4]/td[2]/p[1]/font[1]/input[1]')))\
      .send_keys(date)

    WebDriverWait(driver, 3)\
      .until(EC.element_to_be_clickable((By.XPATH,'/html[1]/body[1]/table[2]/tbody[1]/tr[2]/td[1]/div[1]/center[1]/table[1]/tbody[1]/tr[4]/td[2]/p[1]/font[2]/input[1]')))\
      .send_keys(date)
    
    time.sleep(0.5)

    #Dar clic en Consultar
    WebDriverWait(driver, 3)\
      .until(EC.element_to_be_clickable((By.XPATH,'/html[1]/body[1]/table[2]/tbody[1]/tr[2]/td[1]/div[1]/center[1]/table[1]/tbody[1]/tr[10]/td[1]/p[1]/input[1]')))\
      .click()
    
    time.sleep(0.5)

#Escanea la lista de manifiestos para almacenar solamente sus URLs y luego poder iterar sobre cada URL
def obtenerURLManifiestos(driver):

    #Obtener la longitud de la tabla
    filas = driver.find_element(By.XPATH,'/html[1]/body[1]/center[2]/table[1]/tbody[1]').get_attribute("innerText") 
    filas = filas.replace('\t',',')
    filas = filas.replace('\n\n','') #string

    df = saveTablesInDf(filas)

    #Obtiene la longitud de la tabla
    cant_manifiestos = df.shape[0] 

    #Definimos una lista que va a obtener los URLs de cada manifiesto, empezando por la posición 0
    lista_URL = []

    #Obtiene el URL de cada manifiesto
    for i in range(0,cant_manifiestos):
        URL = driver.find_element(By.XPATH,'/html[1]/body[1]/center[2]/table[1]/tbody[1]/tr['+str(i+2)+']/td[1]/a').get_attribute("href")
        lista_URL.append(URL)
    
    return lista_URL

#Función auxiliar en la que entra tablas raspadas y salen en formato DataFrame
def saveTablesInDf(filas):
    data = StringIO(filas)
    df = pd.read_csv(data,sep=",")
    df1 = pd.DataFrame(df)
    return df1

def convertir_fecha(fecha_str):

    if 'Hora no registrada' in fecha_str or fecha_str == '--/--/----':
        return ''
    try:
        fecha = datetime.strptime(fecha_str, '%d/%m/%Y %H:%M:%S')
        return fecha
    except ValueError:
        try:
            fecha = datetime.strptime(fecha_str, '%d/%m/%Y %H%M')
            return fecha
        except ValueError:
            try:
                fecha = datetime.strptime(fecha_str, '%d/%m/%Y %I:%M:%S %p')
                return fecha
            except ValueError:
                try:
                    fecha = datetime.strptime(fecha_str, '%d/%m/%Y')
                    return fecha
                except ValueError:
                    return fecha_str

def convertir_a_float(cadena):
   return float(cadena.replace(',',''))

def convertir_a_entero(valor):
    try:
        return int(valor)
    except ValueError:
        return None
    
def convertir_detalle_a_numero(valor):
   if('A' in valor):
      valor = '10' + valor[-2:]
   elif ('B' in valor):
      valor = '11' + valor[-2:]
   elif ('C' in valor):
      valor = '12' + valor[-2:]
   elif ('D' in valor):
      valor = '13' + valor[-2:]
   elif ('E' in valor):
      valor = '14' + valor[-2:]
   elif ('F' in valor):
      valor = '15' + valor[-2:]
   elif ('G' in valor):
      valor = '16' + valor[-2:]
   elif ('H' in valor):
      valor = '17' + valor[-2:]
   elif ('I' in valor):
      valor = '18' + valor[-2:]
   elif ('J' in valor):
      valor = '19' + valor[-2:]
   elif ('K' in valor):
      valor = '20' + valor[-2:]
   elif ('L' in valor):
      valor = '21' + valor[-2:]
   elif ('M' in valor):
      valor = '22' + valor[-2:]
   elif ('N' in valor):
      valor = '23' + valor[-2:]
   elif ('O' in valor):
      valor = '24' + valor[-2:]
   elif ('P' in valor):
      valor = '25' + valor[-2:]
   elif ('Q' in valor):
      valor = '26' + valor[-2:]
   elif ('R' in valor):
      valor = '27' + valor[-2:]
   elif ('S' in valor):
      valor = '28' + valor[-2:]
   elif ('T' in valor):
      valor = '29' + valor[-2:]
   elif ('U' in valor):
      valor = '30' + valor[-2:]
   return int(valor)

def getManifestData(driver):
    #Lo colocamos en un try por si no encuentra la tabla (falta visualizar casuísticas especiales)
    try:
      table = driver.find_element(By.XPATH,'/html/body/table/tbody').get_attribute('innerText')
      table = table.replace('í','i')
      table = table.replace(':','')
      table = table.replace('\t','')
      table_tolist = table.split('\n')

      #Se imprime con la cabecera y el valor intercalados y con '', se eliminan esos vacíos.
      for i in range(len(table_tolist) - 1, -1, -1):
        if table_tolist[i] == '':
            table_tolist.pop(i)

      #headers = table_tolist[0::2] 
      headers =  ['Manifiesto', 'Número de Bultos', 'Fecha de Llegada', 'Peso Bruto Kg', 'Fecha de Descarga', 'Matrícula de la Nave', 'Nacionalidad', 'Empresa de Transporte', 'Número de detalles']
      values = table_tolist[1::2] 
      data = [values] 
      df = pd.DataFrame(data, columns=headers)
      df['Número de Bultos'] = df['Número de Bultos'].apply(convertir_a_entero)
      df['Peso Bruto Kg'] = df['Peso Bruto Kg'].apply(convertir_a_float)
      df['Fecha de Descarga'] = df['Fecha de Descarga'].apply(convertir_fecha)
      df['Fecha de Llegada'] = df['Fecha de Llegada'].apply(convertir_fecha)
      df['Número de detalles'] = df['Número de detalles'].apply(convertir_a_entero)
      
      return df

    except Exception as e:
      print("Funcion getManifestData: ",str(e))
      return None

def validateBillOfLadingsTable(driver):
    try:
      driver.find_element(By.XPATH,'/html/body/table[2]/tbody')
      return 1
    except:
      return 0

def cruzar_dataframes(df1, df2, COLUMN): #Puerto de Embarque / Puerto de Destino
  try:
    
    KEY_COLUMN = 'Código de '+ COLUMN
    # Realizamos el cruce de dataframes.
    df = pd.merge(df1, df2, left_on=KEY_COLUMN,right_on='LOCODE', how='left')
    columna1 = "Nombre de " + COLUMN
    columna2 = "País de "+ COLUMN
    columna3 = "Continente de " + COLUMN

    # Rellenamos las columnas de información del puerto de origen con los valores del segundo dataframe.
    df[columna1] = df["NOMBRE"].fillna(df["NOMBRE"])
    df[columna2] = df["PAIS"].fillna(df["PAIS"])
    df[columna3] = df["CONTINENTE"].fillna(df["CONTINENTE"])
    del df["LOCODE"]
    del df["NOMBRE"]
    del df["PAIS"]
    del df["CONTINENTE"]

    df = df.drop_duplicates(subset=df.columns,ignore_index=True)

    return df
  except Exception as e:
     print(f"{str(e)} | en la línea | {e.__traceback__.tb_lineno}")
     


def getInformation(driver,df_manifest_data,URL_manifest,df_puertos):
    
    #Recoger la tabla completa de Bill of Ladings
    filas = driver.find_element(By.XPATH,'/html/body/table[2]/tbody').get_attribute('innerText')
    filas = filas.replace("ó","o")
    filas = filas.replace(',','')
    filas = filas.replace("\t",",")
    filas = filas.replace('\n\n','')

    #Convertir en dataframe todas las filas de BoL
    df_BoL = saveTablesInDf(filas)

    #Obtenemos los URLs de cada detail en un dataframe que tiene dos columnas: Text y URL
    df_BoL_detail_URL = getDetailsURLsFromBoL(driver,df_BoL.shape[0])

    # La tabla tiene 18 columnas. Solo tomaremos estas, de acuerdo con los requisitos de usuario
    columnas_a_copiar = [0, 2, 3, 4, 7, 8, 9, 10, 11, 14, 16, 17]

    #Si la longitud de ambas tablas es la misma
    if(df_BoL.shape[0]==df_BoL_detail_URL.shape[0]):
      
      # Solo selecciona las columnas necesarias y las copia en otro dataframe
      columnas_copiadas = df_BoL.iloc[:,columnas_a_copiar]
      df_BoL_formato = columnas_copiadas.copy()
      
      #Eliminar los NaN del df
      df_BoL_formato = df_BoL_formato.fillna('')

      #Creo la columna Manifiesto+Detalle en el nodo BoL
      df_BoL_formato['Manifiesto+Detalle'] = None
      df_BoL_formato['Nombre de Puerto de Embarque'] = None
      df_BoL_formato['País de Puerto de Embarque'] = None
      df_BoL_formato['Continente de Puerto de Embarque'] = None
      df_BoL_formato['Nombre de Puerto de Destino'] = None
      df_BoL_formato['País de Puerto de Destino'] = None
      df_BoL_formato['Continente de Puerto de Destino'] = None

      #Poner todos como string
      df_BoL_formato = df_BoL_formato.astype(str)

      #Mostrar la tabla BoL
      headers_BoL = ['Código de Puerto de Embarque','Número de Bill of Lading','Detalle','Código de Terminal',
                     'Peso manifestado','Bultos manifestados','Peso recibido','Bultos recibidos','Fecha de transmisión',
                     'Consignatario','Fecha de Tarja','Código de Puerto de Destino','Manifiesto+Detalle',
                     'Nombre de Puerto de Embarque','País de Puerto de Embarque','Continente de Puerto de Embarque',
                     'Nombre de Puerto de Destino','País de Puerto de Destino','Continente de Puerto de Destino']
      df_BoL_formato.columns = headers_BoL

      # Formatear tipo de datos de las columnas
      df_BoL_formato['Detalle'] = df_BoL_formato['Detalle'].apply(convertir_detalle_a_numero)
      df_BoL_formato['Código de Terminal'] = df_BoL_formato['Código de Terminal'].apply(convertir_a_entero)
      df_BoL_formato['Peso manifestado'] = df_BoL_formato['Peso manifestado'].apply(convertir_a_float)
      df_BoL_formato['Bultos manifestados'] = df_BoL_formato['Bultos manifestados'].apply(convertir_a_entero)
      df_BoL_formato['Peso recibido'] = df_BoL_formato['Peso recibido'].apply(convertir_a_float)
      df_BoL_formato['Bultos recibidos'] = df_BoL_formato['Bultos recibidos'].apply(convertir_a_entero)
      df_BoL_formato['Fecha de transmisión'] = df_BoL_formato['Fecha de transmisión'].apply(convertir_fecha)
      df_BoL_formato['Fecha de Tarja'] = df_BoL_formato['Fecha de Tarja'].apply(convertir_fecha)
      
      
      #Seteando nombre, país y continente de puerto de origen
      df_BoL_formato = cruzar_dataframes(df_BoL_formato, df_puertos, "Puerto de Embarque")
      df_BoL_formato = cruzar_dataframes(df_BoL_formato, df_puertos, "Puerto de Destino")
  
      # Seteando la columna Manifiesto+Detalle
      for i in range(0,df_BoL_formato.shape[0]):
        df_BoL_formato.at[i,'Manifiesto+Detalle'] = str(df_manifest_data.iloc[0,0]) + " DETALLE " + str(df_BoL_formato.at[i,'Detalle'])      
      
      #Guardar cada fila del df_BoL_formato en una lista de df
      list_BoL_formato = [] 

      # Iterar a través de cada fila del DataFrame y guardarla en la lista
      for i in range(len(df_BoL_formato)):
        fila_actual = df_BoL_formato.iloc[i]
        df_fila_actual = pd.DataFrame([fila_actual.to_dict()])  # Crear un DataFrame a partir de la fila
        list_BoL_formato.append(df_fila_actual)

      for i in range(0,df_BoL_detail_URL.shape[0]): #evaluar el -1

        # Aquí df_BoL_formato ya está preparado para ser enviado a Neo4J
        insertNode(list_BoL_formato[i],"Bill of Lading","Manifiesto+Detalle")
        time.sleep(0.5)
        relateNodes("Manifiesto de carga",df_manifest_data.at[0,"Manifiesto"],list_BoL_formato[i].at[0,"Manifiesto+Detalle"])

        #Abrir la página en la que se encuentran el detalle y el contenedor
        print("df BoL detail URL [",i,"]")
        driver.get(df_BoL_detail_URL.at[i, 'URL'])
        time.sleep(0.5) #2

        #Solo si hay información del contenedor, guardarla, sino pasar
        try:
          #Obtenemos cantidad de filas de bultos
          filas_bultos = driver.find_elements(By.XPATH,'/html[1]/body[1]/table[1]/tbody[1]/tr')
          num_filas_bultos = len(filas_bultos)-1 # quitarle la cabecera

          filas_4 = driver.find_element(By.XPATH,'/html[1]/body[1]/table[1]/tbody[1]').get_attribute("innerHTML")
          soup = BeautifulSoup(filas_4, "html.parser")
          
          filas_4_anexo = []
          for row in soup.find_all("tr"):
              celdas = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
              filas_4_anexo.append(celdas)

          del filas_4_anexo[0]

          # for fila in filas_4_anexo:
          #   print(fila)

          headers_bultos =  ['Cantidad de Bultos', 'Peso Bruto Kg', 'Empaques', 'Embarcador', 'Consignatario', 'Marcas y Números', 'Descripción de Mercadería']
          df_filas_4_new = pd.DataFrame(filas_4_anexo,columns = headers_bultos)

          df_filas_4_new["Cantidad de Bultos"] = df_filas_4_new["Cantidad de Bultos"].apply(convertir_a_entero)
          df_filas_4_new["Peso Bruto Kg"] = df_filas_4_new["Peso Bruto Kg"].apply(convertir_a_float)
          
          #Lo eliminamos para que al relacionar Bulto con Bill of Lading, no haya problemas, ya que tienen la misma columna
          del df_filas_4_new["Consignatario"]
          
          #Insertar el nodo
          lista_idbulto= insertNodev2(df_filas_4_new,"Bultos")
          #Si hay varios bultos, tendremos un arreglo de varios elementos | Si hay un bulto, tendré un arreglo de un elemento

          lista_bultos = []
          # Iterar a través de cada fila del DataFrame y guardarla en la lista
          for r in range(0,num_filas_bultos):
            fila_actual = df_filas_4_new.iloc[r]
            df_fila_actual = pd.DataFrame([fila_actual.to_dict()])  # Crear un DataFrame a partir de la fila
            lista_bultos.append(df_fila_actual)

          #Relacionar los bultos con su respectivo BoL
          for a in range(0,len(lista_idbulto)):
            relateNodes("Bill of Lading",list_BoL_formato[i].at[0,"Manifiesto+Detalle"],lista_idbulto[a])

          # Obtenemos el número de contenedores
          filas_contenedores = driver.find_elements(By.XPATH,'/html[1]/body[1]/table[2]/tbody[1]/tr')
          num_filas_contenedores = len(filas_contenedores)-1
          
          #Obtenemos el contenedor
          filas_3 = driver.find_element(By.XPATH,'/html[1]/body[1]/table[2]/tbody[1]').get_attribute("innerText")
          filas_3 = filas_3.replace(',','')
          filas_3 = filas_3.replace("\t",",")
          filas_3 = filas_3.replace('\n\n','')
          df_containers = saveTablesInDf(filas_3)
          df_containers = df_containers.fillna('')
          df_containers = df_containers.astype(str)

          df_containers['Tamaño'] = df_containers['Tamaño'].apply(convertir_a_entero)
          df_containers['Condición'] = df_containers['Condición'].apply(convertir_a_entero)
          df_containers['Tipo'] = df_containers['Tipo'].apply(convertir_a_entero)
          df_containers['Tara'] = df_containers['Tara'].apply(convertir_a_entero)
          #print(tabulate(df_containers, headers='keys', tablefmt='psql'))

          list_containers = []

          # Iterar a través de cada fila del DataFrame y guardarla en la lista
          for m in range(0,len(df_containers)):
            fila_actual = df_containers.iloc[m]
            df_fila_actual = pd.DataFrame([fila_actual.to_dict()])  # Crear un DataFrame a partir de la fila
            list_containers.append(df_fila_actual)

          for v in range(0,len(list_containers)):
            #Setear el estado
            list_containers[v]["Estado"] = "DES"

          #Insertar contenedor a BD Neo4J y SQL Server
          for k in range (0,len(list_containers)):
            insertNode(list_containers[k],"Contenedor","Número")
            #Insertar a SQL Server
            valor = c.validate(list_containers[k])
            if(valor==0):
              c.insert(list_containers[k])
            elif(valor==1):
              c.update(list_containers[k])
              print("Ya existe ese valor en la bd.")
            
          #IMPLEMENTAR LÓGICA PARA CADA CASUÍSTICA DE RELACIÓN BULTO(S)-CONTENEDOR(ES)
          if(num_filas_contenedores == 1 and num_filas_bultos == 1):
             #RELACIÓN DE 1 A 1 (1 - 1)
             relateNodes("Bultos",lista_idbulto[0],list_containers[0].at[0,"Número"])
          elif(num_filas_contenedores != 1 and (num_filas_contenedores == num_filas_bultos)):
             #RELACIÓN DE MUCHOS A MUCHOS (n - n)
             for u in range(0,num_filas_contenedores):
                relateNodes("Bultos",lista_idbulto[u],list_containers[u].at[0,"Número"])
          elif(num_filas_bultos == 1 and num_filas_contenedores>1):
             #RELACIÓN DE 1 A MUCHOS (1 - n)
             for v in range(0,num_filas_contenedores):
                relateNodes("Bultos",lista_idbulto[0],list_containers[v].at[0,"Número"])

        except Exception as e:
          print(f"{str(e)} | en la línea | {e.__traceback__.tb_lineno}")
          pass
        
        driver.get(URL_manifest)



def getDetailsURLsFromBoL(driver,count_BoL):
    detail_number_list = []
    URL_list = []

    #Obtener los URLs de cada detalle del manifiesto
    for i in range(0,count_BoL):
      td = '/html/body/table[2]/tbody/tr['+str(i+2)+']/td[4]'
      detail_number = driver.find_element(By.XPATH,td).get_attribute("innerText")
      link = driver.find_element(By.XPATH,td+'/a').get_attribute("href")
      detail_number_list.append(detail_number)
      URL_list.append(link)
    
    data = {
        'Text': detail_number_list,
        'URL': URL_list
    }
    df = pd.DataFrame(data)
    return df
    
#ESTA FUNCION NO TOCARLA, ESTÁ FUNCIONANDO PERFECTO SOBRE LA TABLA 2 DE BoL
def convertBoLTableToDf(row):
    # Crear un objeto BeautifulSoup
    soup = BeautifulSoup(row, 'html.parser')

    # Encontrar todas las filas de la tabla
    filas = soup.find_all('tr')

    # Listas para almacenar los datos
    data = []
    encabezados = []

    # Recorrer las filas para extraer los datos
    for index, fila in enumerate(filas):
        celdas = fila.find_all('td')
        if index == 0:  # La primera fila contiene los encabezados
            encabezados = [celda.get_text(strip=True) for celda in celdas]
        else:
            datos_fila = [celda.get_text(strip=True) for celda in celdas]
            data.append(datos_fila)

    # Crear un DataFrame usando pandas
    df = pd.DataFrame(data, columns=encabezados)

    return df


if __name__ == "__main__":
    
    #CONFIGURACIONES DEL DRIVER Y NAVEGADOR FIREFOX
    link = 'http://www.aduanet.gob.pe/aduanas/informao/HRMCFLlega.htm'

    #Obtener info de paises y puertos
    df_puertos = pd.read_csv(ruta_puertos,sep=";",encoding="latin-1")

    #Inicializamos el driver de Firefox
    driver = accessDriver(link,driver_location,firefox_location)

    #Ingresar datos y consultar
    enterDates(driver)

    #Obtenemos lista de URLs de manifiestos
    lista_URL = obtenerURLManifiestos(driver)

    #Iteramos por cada URL
    for URL in lista_URL:
        #Ingresamos a cada Manifiesto
        driver.get(URL)
        time.sleep(1)
        
        # Primera tabla: Nodo MANIFIESTO DE CARGA
        df_manifest_data = getManifestData(driver)
        df_manifest_data = df_manifest_data.fillna('')

        #Ingresar nodo de Manifiesto a Neo4J
        insertNode(df_manifest_data,"Manifiesto de carga","Manifiesto")

        #Verificar si tiene tabla de Bill of Ladings
        int_validate = validateBillOfLadingsTable(driver)

        #Si la tabla no existe (0) se va al siguiente URL
        if(int_validate==0):  
           continue
        else: 
           #Si el valor existe, extrae la tabla y la mantiene
           getInformation(driver,df_manifest_data,URL,df_puertos)
        