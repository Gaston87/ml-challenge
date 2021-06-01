# Challenge Técnico - Gestión Operativa

## Problema presentado
Se tiene la necesidad de consultar y modificar información masivamente. Para poder agilizar estas tareas necesitamos construir un script que nos permita realizar la siguiente tarea:
Recorrer todos los ítems publicados de los siguientes users:
* site_id, seller_id
* MLA,326659539
* MLB,244066576

Consideraciones:
1. Utilizar el siguiente filtro para obtener solamente los que cumplan esta condición "Envío" = "Full"
2. Generar archivo de LOG que contengan los siguientes datos de cada ítem por cada
user/site:
site_id, seller_id, nickname, id del ítem, title del item, category_id (* donde está
publicado *), name de la categoría (* no el id *), currency_symbol + price
EJEMPLO
MLA, 99999, SampleNickname, MLA999999, "Sample title", MLA11111, "Herramientas", $99
MLB, 22222, SampleNickn2, MLB22221, "Sample title 2", MLB2222, "Herramientas", R$33


## Solución

Se creó un script Python el cual recibe un archivo input en formato CSV para listar los site_id y seller_id requeridos. El script genera un archivo con la siguiente nomenclatura "log DD MM AAAA - HH MM.csv"(Ejemplo 31 05 2021 - 05 30.csv). 
Para mas información, el script genera(o actualiza si ya existe) el archivo "ExecutionLog.txt" en el cual se muestra la secuencia de ejecución del script desde que es iniciado hasta su finalización, indicando fecha y hora de cada registro y errores si ocurrieran dentro de la ejecución.

## Prerequisitos

1. Contar con Python 3.9.5 o superior. Link de descarga: [Python](https://www.python.org/downloads/)

2. Configurar el archivo brindado 'properties.ini'. Dentro de la sección [SETUP] se debe ingresar las claves para los atributos de 'client_id' y 'client_secret'.

## Instalación

Dentro de la consola escribir el siguiente comando:
```python
pip install -r requisitos.txt
```

## Comandos script

Help:
```python
py script.py -h
```

Ejecución:
```python
py script.py -i input.csv
```