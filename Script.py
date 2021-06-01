##############################################################################################################################
#TITULO: 		Challenge Técnico - Gestión Operativa
#AUTOR:	 		GASTON RUGGERI
#DESCRIPCION:	El desarrollo recorre un archivo dado por parametro que contiene identificaciones de vendedores.
#				De cada vendedor consulta sus items, filtrando solo aquellos que tengan envío full y genera un 
#				archivo csv de salida con el siguiente formato:
#				site_id, seller_id, nickname, id del ítem, title del item, category_id, category_name,currency_symbol + price
##############################################################################################################################

##############################################################################################################################
#DECLARACION DE BIBLIOTECAS
##############################################################################################################################
from datetime import datetime, timedelta #Para calculos de fecha y hora
from multiprocessing import Pool
import configparser		# Para leer de properties.ini
import sys, getopt		# Para manejo del input/output por argumento de línea de comando
import requests as req	# consumir apis rest
import csv              # Para leer csv
import json		        # Para trabajar con formato json
import os				# Para caracteres de sistema segun OS

##############################################################################################################################
#VARIABLES GLOBALES
##############################################################################################################################
#Data para token
url_base 		= 'https://api.mercadolibre.com/'
token	 		= None
client_id		= ''
client_secret 	= ''
#Diccionarios 
currencies = {}
categories = {}
#Variables
txt_ExecutionLog = ''
filtro_envios = 'fulfillment' #Usa en API search para filtrar por ENVÍO = FULL
executionlog = 'ExecutionLog.txt'

##############################################################################################################################
#BLOQUES DE FUNCIONES
##############################################################################################################################

#Función principal del Script
def execute(cmd_parameters):
	try:
		global txt_ExecutionLog   #Para escribir la variable global
		txt_ExecutionLog =  initialize_file(executionlog)
		get_config_data('properties.ini')
		run_process(cmd_parameters)
	except:
		print("Ha ocurrido un error:", sys.exc_info()[1])

#Obtiene la configuración para variables de API
def get_config_data(config_name):
		executionlog_insert('Inicia un nuevo proceso')
		config = configparser.ConfigParser() #Crea el objeto para parsear el properties.ini
		config.read(config_name)
		global client_id	 #Para escribir la variable global
		global client_secret #Para escribir la variable global
		client_id 	  = config['SETUP']['client_id']
		client_secret = config['SETUP']['client_secret']
		executionlog_insert('Parametros de configuración leídos correctamente')

#Función con el proceso de generación de archivo LOG
def run_process(cmd_parameters):
	global currencies         #Para escribir la variable global
	
	#Obtiene las monedas
	currencies = get_currencies()

	# Manejo de archivos de input/output
	input_file_name  = get_input_path(cmd_parameters)
	csv_input_file 	 = read_csv_file(input_file_name, ['site_id','seller_id'])
	output_file_name = get_file_name() #Crea nombre de archivo de salida con nombre de fecha actual
	csv_output_file  = create_csv_file(output_file_name, ['site_id','seller_id','nickname','item_id','item_title','category_id','category_name','price'])

	#Iteración por cada registro del archivo input
	for csv_input_data in csv_input_file:
		site_id 		= csv_input_data['site_id']
		seller_id 		= csv_input_data['seller_id']
		search_result	= get_search_seller(site_id, seller_id, filtro_envios)
		seller			= search_result['seller']
		items			= search_result['results']
		executionlog_insert('Comenzó proceso para sitio id: ' + site_id + ' Seller_id: ' + seller_id)
		print('Comenzó proceso para sitio id: ' + site_id + ' Seller_id: ' + seller_id)

		#Buffer de categorias para lectura adelantada
		global categories
		category_ids = [item["category_id"] for item in items] #Armos listado de categorias
		categories.update(get_categorys_by_results(list(dict.fromkeys(category_ids)))) #Consulta por listado de categorias sin duplicados

		#Iteración por cada item resultado de la consulta
		for item in items:
			try:
				csv_output_file.writerow(prepare_report_line(site_id, seller, item))
			except:
				print("Ha ocurrido un error para {0},{1},{2}:{3}".format(site_id, seller_id, item['id'], sys.exc_info()))
				executionlog_insert("Ha ocurrido un error para {0},{1},{2}:{3}".format(site_id, seller_id, item['id'], sys.exc_info()[1]))
	print("Se generó el reporte: {0}".format(output_file_name))
	executionlog_insert("Se generó el reporte: {0}".format(output_file_name))
	executionlog_insert("Proceso finalizado correctamente")

#Función para cargar cabecera de consulta
def get_auth_header():
	return {'Authorization': 'Bearer {0}'.format(get_token()['access_token'])}

#Devuelve token activo
def get_token():
	global token #Para escribir la variable global
	if token == None or hasExpired(token):
		token = post_oauth()
	return token

#Token vigente?
def hasExpired(token):
	return datetime.now() > token['expiry_date']

#Obtener Token
def post_oauth():
	uri = 'https://api.mercadolibre.com/oauth/token?grant_type=client_credentials&client_id={0}&client_secret={1}'.format(client_id, client_secret)
	response = req.post(uri)
	response.raise_for_status()
	token = json.loads(response.text)
	token['expiry_date'] = datetime.now()+timedelta(seconds=token['expires_in']) #Agrego el expiry_date para saber cúando expira y renovarlo de ser necesario
	return token 

def get_search_seller(site_id, seller_id, shipping):
	results = []
	offset  = 0

	search_result = get_search_seller_offset(site_id, seller_id, shipping, offset)
	results.extend(search_result['results'])  #Agrega a resultados la primera consulta
	offset = offset + search_result['paging']['limit'] #Valor con el que pagina(50 en este caso)
	while search_result['paging']['total'] > offset:
		search_result = get_search_seller_offset(site_id, seller_id, shipping, offset)
		results.extend(search_result['results']) #Agrega a resultados las iteraciones de la consulta
		offset = offset + search_result['paging']['limit']
	return {'seller':search_result['seller'], 'results':results} #Retorna Seller y resultados para ser grabados en csv

def get_search_seller_offset(site_id, seller_id, shipping, offset):
	uri = 'https://api.mercadolibre.com/sites/{0}/search?seller_id={1}&shipping={2}&offset={3}'.format(site_id, seller_id, shipping, offset)
	response = req.get(uri, headers=get_auth_header()) #Hace el request 
	response.raise_for_status()
	return json.loads(response.text) #Retorna respuesta en formato json

def get_currency_symbol(currency_id):
	return currencies[currency_id] #Busca la clave en diccionario y retorna valor

#Función para obtener las combinaciones de MONEDA/SIMBOlO, devuelto en un diccionario  	
def get_currencies():
	uri = 'https://api.mercadolibre.com/currencies'
	response = req.get(uri)
	response.raise_for_status()
	return to_map(json.loads(response.text), 'id', 'symbol')

def get_category_name(category_id):
	return categories[category_id]

def get_category(category_id):
	uri = 'https://api.mercadolibre.com/categories/{0}'.format(category_id)
	print(" "+uri)
	response = req.get(uri)
	response.raise_for_status()
	return json.loads(response.text)

#Abre el archivo input para lectura
def read_csv_file(file_name, fieldnames):
	input_file = open(file_name, 'r') #Abre el archivo en modo lectura
	return csv.DictReader(input_file, fieldnames=fieldnames)

#Abre archivo de salida en modo escritura
def create_csv_file(file_name, fieldnames):
	output_file = open(file_name, 'w', newline='')
	return csv.DictWriter(output_file, fieldnames=fieldnames) #En la respuesta mapeo el formato de csv que recibe por fieldnames

#Inicializa archivo para LOG de ejecución
def initialize_file(filename):
	execution_log = open(filename, 'a') #Abre archivo en modo anexar
	return execution_log

def executionlog_insert(text):
	format = "%a %b %d %Y %H:%M:%S" 
	today = datetime.today()
	line = today.strftime(format) + ' ' + text
	txt_ExecutionLog.write(line + os.linesep)

def close_file(filename):
	filename.close()

def to_map(results, key, value):
	map = {} 
	for result in results:
		map[result[key]] = result[value]
	return map

def prepare_report_line(site_id, seller, item):
	data_for_report = {
		'site_id': site_id, 
		'seller_id': seller['id'], 
		'nickname': seller['nickname'], 
		'item_id': item['id'], 
		'item_title': item['title'], 
		'category_id': item['category_id'], 
		'category_name': get_category_name(item['category_id']), 
		'price': get_currency_symbol(item['currency_id']) + str(item['price']) 
	}
	return data_for_report

def get_file_name():
    format = "%d %m %y - %H %M"     # Asigna formato para fecha
    today = datetime.today()        # Asigna fecha-hora actual
    return 'Log ' + today.strftime(format) + '.csv' 

#Procesa los parametros de input de la linea de comando
def get_input_path(cmd_parameters):
	try:
		opts, args = getopt.getopt(cmd_parameters,"hi:",["ifile="])
	except getopt.GetoptError:
		print('Script.py -i <inputfile.csv>')
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print('Script.py -i <inputfile.csv>')
			sys.exit()
		elif opt in ("-i", "--ifile"):
			return arg

#Por multiprocesamiento obtengo los categories names
def get_categorys_by_results(category_ids):
	category_ids = [category_id for category_id in category_ids if category_id not in categories.keys()]
	pool = Pool(10)
	return to_map(pool.map(get_category, category_ids), 'id', 'name') #Pool.map() Simil for pero multiproceso

# Ejecutar funcion principal
if __name__ == '__main__':
	execute(sys.argv[1:])        