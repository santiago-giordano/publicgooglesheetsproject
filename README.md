# ALKEMY CHALLENGE: Data analytics + Python

![python version](https://img.shields.io/badge/python-v3.10.5-blue)

Este proyecto responde a las consignas del desafío que Alkemy propone para su "Aceleración" en Data analytics con Python. La Aceleración en Alkemy es un entrenamiento en donde desarrolladores de software trainee Aceleran y Certifican sus habilidades como programadores Junior productivos en Laboratorios de Práctica.

## Pre-requisitos:
Antes de continuar, asegurate de cumplir los siguientes requerimientos:

* Instalar la última versión de Python (aquí fue usado Python 3.10.5)
* Crear y/o activar el entorno virtual
* Asegurarse de tener pip actualizado en el entorno virtual
* Instalar los siguientes módulos, paquetes y librerías en el entorno virtual:
  * requests
  * datetime
  * openpyxl
  * pandas
  * sqlalchemy
  * python-decouple
* Configurar la conexión a la base de datos PostreSQL (archivo <.env>)
* Finalmente ejecutar el archivo <AlkemyChallenge-v4.3.py>

> En el archivo <deploy.py> se encuentran los códigos necesarios para ejecutar los diversos pasos de la instalación y ejecución.
> Para configurar la conexión se debe acceder al archivo <.env> y editar el usuario, la clave y el host del servidor PostgreSQL que se vaya a utilizar.

La ejecución del archivo <AlkemyChallenge-v4.3.py> creará en la raiz del proyecto tres carpetas donde se guardarán los archivos adquiridos en formato csv. Luego procesará los datos de los archivos fuente creando cinco dataframes, creará la conexión al servidor PostgreSQL, creará la base de datos "alkemy" (si no existe), creará las tablas (si no existen) y exportará los datos a sus respectivas tablas, actualizando los datos (si ya habían sido exportados).
A su vez se incorporó el código que exporta los cinco dataframes a archivos excel.

## Información de contacto:
Mail: giordanomalta@gmail.com
LinkedIn: https://www.linkedin.com/in/santiagogiordanomalta/
GitHub: https://github.com/santiago-giordano
