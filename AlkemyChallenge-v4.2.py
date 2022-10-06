#!/usr/bin/env python
# coding: utf-8


# # **1. Importación de las librerías y almacenamiento local de los archivos csv**
#
# *   Librerías: Pandas y Requests
# *   Fuente de los datos: Google Sheets (public)
#
# Los archivos fuentes serán utilizados en tu proyecto para obtener de ellos todo lo
# necesario para popular la base de datos. El proyecto deberá:
# ### ● Obtener los 3 archivos de fuente utilizando la librería requests y
# almacenarse en forma local (Ten en cuenta que las urls pueden cambiar en
# un futuro):
#
# *   Datos Argentina - Museos
# *   Datos Argentina - Salas de Cine
# *   Datos Argentina - Bibliotecas Populares
#
#
# ### ● Organizar los archivos en rutas siguiendo la siguiente estructura:
# “categoría\año-mes\categoria-dia-mes-año.csv”
# * Por ejemplo: “museos\2021-noviembre\museos-03-11-2021”
# * Si el archivo existe debe reemplazarse. La fecha de la nomenclatura
# es la fecha de descarga.


# Se importan las librerías y módulos
# In[1]:

import pandas as pd
import requests
from datetime import date
from sqlalchemy import create_engine

# Se obtienen los 3 archivos fuente
# In[2]:

today = date.today()
today = today.strftime("%d-%m-%Y")

# Se obtiene el archivo fuente de "museos"
r = requests.get(
    "https://docs.google.com/spreadsheet/ccc?key=1PS2_yAvNVEuSY0gI8Nky73TQMcx_G1i18lm--jOGfAA&output=csv"
)
open(f"D:\DATA\Alkemy\museos\\2022-octubre\museos-{today}.csv", "wb").write(r.content)
df1 = pd.read_csv(f"D:\DATA\Alkemy\museos\\2022-octubre\museos-{today}.csv")
df1["subcategoria"].fillna("Museos", inplace=True)

# Se obtiene el archivo fuente de "cines"
r = requests.get(
    "https://docs.google.com/spreadsheet/ccc?key=1o8QeMOKWm4VeZ9VecgnL8BWaOlX5kdCDkXoAph37sQM&output=csv"
)
open(f"D:\DATA\Alkemy\cines\\2022-octubre\cines-{today}.csv", "wb").write(r.content)
df2 = pd.read_csv(f"D:\DATA\Alkemy\cines\\2022-octubre\cines-{today}.csv")

# Se obtiene el archivo fuente de "bibliotecas"
r = requests.get(
    "https://docs.google.com/spreadsheet/ccc?key=1udwn61l_FZsFsEuU8CMVkvU2SpwPW3Krt1OML3cYMYk&output=csv"
)
open(f"D:\DATA\Alkemy\\bibliotecas\\2022-octubre\\bibliotecas-{today}.csv", "wb").write(
    r.content
)
df3 = pd.read_csv(f"D:\DATA\Alkemy\\bibliotecas\\2022-octubre\\bibliotecas-{today}.csv")


# # **2. Procesamiento de los datos**

# > ### A. Normalizar toda la información de Museos, Salas de Cine y Bibliotecas
# Populares, para crear una única tabla que contenga:
#
# *   cod_localidad
# *   id_provincia
# *   id_departamento
# *   categoría
# *   provincia
# *   localidad
# *   nombre
# *   domicilio
# *   código postal
# *   número de teléfono
# *   mail
# *   web
#

# I. Se normaliza el dataframe de museos:

# Antes se crea el dataframe "df1_fuente", necesario para la creación del dataframe "df_fuente"
# In[7]:
df1_fuente = df1[:]

# Se eliminan las columnas innecesarias:
# In[11]:
df1.drop(
    columns=[
        "Observaciones",
        "categoria",
        "piso",
        "cod_area",
        "Latitud",
        "Longitud",
        "TipoLatitudLongitud",
        "Info_adicional",
        "fuente",
        "jurisdiccion",
        "año_inauguracion",
        "actualizacion",
    ],
    inplace=True,
)

# Se renombran las columnas restantes según indicaciones de normalización:
# In[8]:
df1.rename(
    columns={
        "Cod_Loc": "cod_localidad",
        "IdProvincia": "id_provincia",
        "IdDepartamento": "id_departamento",
        "subcategoria": "categoria",
        "direccion": "domicilio",
        "CP": "codigo_postal",
        "telefono": "numero_telefono",
        "Mail": "mail",
        "Web": "web",
    },
    inplace=True,
)

# Se cambian los tipos de datos del dataframe resultante:
# In[9]:
df1 = df1.astype("string")

# II. Se normaliza el dataframe de cines:

# Antes se crea el dataframe "df2_fuente", necesario para la creación del dataframe "df_fuente"
# In[11]:
df2_fuente = df2[:]

# Se eliminan las columnas innecesarias:
# In[11]:
df2.drop(
    columns=[
        "Observaciones",
        "Departamento",
        "Piso",
        "cod_area",
        "Latitud",
        "Longitud",
        "TipoLatitudLongitud",
        "Información adicional",
        "Fuente",
        "tipo_gestion",
        "Pantallas",
        "Butacas",
        "espacio_INCAA",
        "año_actualizacion",
    ],
    inplace=True,
)

# Se renombran las columnas restantes según indicaciones de normalización:
# In[12]:
df2.rename(
    columns={
        "Cod_Loc": "cod_localidad",
        "IdProvincia": "id_provincia",
        "IdDepartamento": "id_departamento",
        "Categoría": "categoria",
        "Provincia": "provincia",
        "Localidad": "localidad",
        "Nombre": "nombre",
        "Dirección": "domicilio",
        "CP": "codigo_postal",
        "Teléfono": "numero_telefono",
        "Mail": "mail",
        "Web": "web",
    },
    inplace=True,
)

# Se cambian los tipos de datos del dataframe resultante:
# In[13]:
df2 = df2.astype("string")


# III. Se normaliza el dataframe de bibliotecas:

# Antes se crea el dataframe "df3_fuente", necesario para la creación del dataframe "df_fuente"
# In[16]:
df3_fuente = df3[:]

# Se eliminan las columnas innecesarias:
# In[11]:
df3.drop(
    columns=[
        "Observacion",
        "Subcategoria",
        "Departamento",
        "Piso",
        "Cod_tel",
        "Latitud",
        "Longitud",
        "TipoLatitudLongitud",
        "Información adicional",
        "Fuente",
        "Tipo_gestion",
        "año_inicio",
        "Año_actualizacion",
    ],
    inplace=True,
)

# Se renombran las columnas restantes según indicaciones de normalización:
# In[17]:
df3.rename(
    columns={
        "Cod_Loc": "cod_localidad",
        "IdProvincia": "id_provincia",
        "IdDepartamento": "id_departamento",
        "Categoría": "categoria",
        "Provincia": "provincia",
        "Localidad": "localidad",
        "Nombre": "nombre",
        "Domicilio": "domicilio",
        "CP": "codigo_postal",
        "Teléfono": "numero_telefono",
        "Mail": "mail",
        "Web": "web",
    },
    inplace=True,
)

# Se cambian los tipos de datos del dataframe resultante:
# In[18]:
df3 = df3.astype("string")

# IV. Se unen los tres dataframes en una única tabla:
# In[20]:
frames1 = [df1, df2, df3]
df = pd.concat(frames1, ignore_index=True)

# Se crea una función para normalizar el dataframe
# In[22]:
def correct(x):
    if x == "Santa Fé":
        return "Santa Fe"
    elif x == "Tierra del Fuego":
        return "Tierra del Fuego, Antártida e Islas del Atlántico Sur"
    else:
        return x


# Se aplica la función
# In[23]:
df["provincia"] = df["provincia"].apply(correct)

# Se cambia el tipo de datos de la columna "provincia"
# In[24]:
df["provincia"] = df["provincia"].astype("string")

# Se exporta el dataframe a un nuevo archivo excel
# In[25]:
df.to_excel("tabla única - museos_cines_bibliotecas.xlsx", sheet_name="Tabla única")


# > ### B. Procesar los datos conjuntos para poder generar una tabla con la siguiente información:
# * Cantidad de registros totales por categoría
# * Cantidad de registros totales por fuente
# * Cantidad de registros por provincia y categoría


# I. Se crea la tabla "Cantidad de registros totales por categoría"
# In[27]:
df_countbycat = df.pivot_table(index="categoria", aggfunc="count", values="nombre")

# Se resetea el index y se normaliza el dataframe
# In[28]:
df_countbycat.reset_index(inplace=True)
df_countbycat.rename(
    columns={"categoría": "categoria", "nombre": "cantidad"}, inplace=True
)

# Se exporta el dataframe a un nuevo archivo excel
# In[29]:
df_countbycat.to_excel(
    "registros totales por categoría.xlsx", sheet_name="RT por Categoría"
)


# II. Se crea el dataframe "df_fuente", necesario para la tabla "Cantidad de registros totales por fuente"
df2_fuente.head(1)
# Se normalizan los dataframes
# In[20]:
df1_fuente = df1_fuente[["categoria", "fuente"]]
df1_fuente = df1_fuente.astype("string")

df2_fuente = df2_fuente[["Categoría", "Fuente"]]
df2_fuente.rename(
    columns={"Categoría": "categoria", "Fuente": "fuente"},
    inplace=True,
)
df2_fuente = df2_fuente.astype("string")

df3_fuente = df3_fuente[["Categoría", "Fuente"]]
df3_fuente.rename(
    columns={"Categoría": "categoria", "Fuente": "fuente"},
    inplace=True,
)
df3_fuente = df3_fuente.astype("string")

# Se unen los tres dataframes en una única tabla:
# In[20]:
frames2 = [df1_fuente, df2_fuente, df3_fuente]
df_fuente = pd.concat(frames2, ignore_index=True)

# Se normalizan las fuentes con una función
# In[29]:
def normal(x):
    if x == "Gobierno de la provincia":
        return "Gobierno de la Provincia"
    elif x == "Gob. Pcia.":
        return "Gobierno de la Provincia"
    else:
        return x


# Se aplica la función
# In[29]:
df_fuente["fuente"] = df_fuente["fuente"].apply(normal)

# Se convierte el dataframe en la tabla "Cantidad de registros totales por fuente"
# In[20]:
df_fuente = df_fuente.pivot_table(index="fuente", aggfunc="count", values="categoria")

df_fuente.rename(
    columns={
        "categoria": "cantidad",
    },
    inplace=True,
)

df_fuente.sort_values("cantidad", inplace=True, ascending=False)
df_fuente.reset_index(inplace=True)


# Se exporta el dataframe a un nuevo archivo excel
# In[29]:
df_fuente.to_excel("registros totales por fuente.xlsx", sheet_name="RT por Fuente")


# III. Se crea la tabla "Cantidad de registros por provincia y categoría"
# In[31]:
df_countbyprov = df.pivot_table(
    index=["categoria", "provincia"], aggfunc="count", values="nombre"
)

# Se resetea el index y se normaliza el dataframe
# In[32]:
df_countbyprov.reset_index(inplace=True)
df_countbyprov.rename(
    columns={"categoría": "categoria", "nombre": "cantidad"}, inplace=True
)

# Se exporta el dataframe a un nuevo archivo excel
# In[34]:
df_countbyprov.to_excel(
    "registros por provincia y categoría.xlsx", sheet_name="Por Prov-Cat"
)


# > ### C. Procesar la información de cines para poder crear una tabla que contenga:
# * Provincia
# * Cantidad de pantallas
# * Cantidad de butacas
# * Cantidad de espacios INCAA

# Se obtiene el archivo fuente de "cines"
# In[35]:
r = requests.get(
    "https://docs.google.com/spreadsheet/ccc?key=1o8QeMOKWm4VeZ9VecgnL8BWaOlX5kdCDkXoAph37sQM&output=csv"
)
open("df_cines.csv", "wb").write(r.content)
df_cines = pd.read_csv("df_cines.csv")

# Se seleccionan las columnas necesarias
# In[36]:
df_cines = df_cines[["Provincia", "Pantallas", "Butacas", "espacio_INCAA"]]
df_cines.head()

# Se cambian los valores null para luevo poder sumar los registros
# In[37]:
df_cines.fillna("0", inplace=True)

# Se muestran los valores únicos de la columna "espacio_INCAA" para evaluar si es necesario normalizar
# In[39]:
a = df_cines["espacio_INCAA"].unique()
print(sorted(a))

# Se crea una función para normalizar la columna
# In[40]:
def unique(x):
    if x == "si":
        return "1"
    elif x == "SI":
        return "1"
    else:
        return x


# Se aplica la función
# In[41]:
df_cines["espacio_INCAA"] = df_cines["espacio_INCAA"].apply(unique)

# Se modifican los tipos de datos
# In[43]:
df_cines["Provincia"] = df_cines["Provincia"].astype("string")
df_cines["espacio_INCAA"] = df_cines["espacio_INCAA"].astype("int64")
df_cines.dtypes

# Se muestran los valores únicos de la columna "Provincia" para evaluar si es necesario normalizar
# In[44]:
a = df_cines["Provincia"].unique()
print(sorted(a))

# Se crea la tabla con la suma de "Pantallas", "Butacas" y "espacio_INCAA"
# In[45]:
df_cines = df_cines.pivot_table(
    index="Provincia", aggfunc="sum", values=["Pantallas", "Butacas", "espacio_INCAA"]
)

# Se resetea el index del dataframe df_cines para incluir "Provincia" como columna
# In[46]:
df_cines.reset_index(inplace=True)
df_cines.dtypes

# Se exporta el dataframe a un nuevo archivo excel
# In[778]:
df_cines.to_excel("cines.xlsx", sheet_name="cines")


# # 3. Creación de la base de datos y sus tablas:
# Para disponibilizar la información obtenida y procesada en los pasos previos, tu
# proyecto deberá tener una base de datos que cumpla con los siguientes requisitos:
# * La base de datos debe ser PostgreSQL
# * Se deben crear los scripts .sql para la creación de las tablas.
# * Se debe crear un script .py que ejecute los scripts .sql para facilitar el deploy.
# * Los datos de la conexión deben poder configurarse fácilmente para facilitar el deploy en un nuevo ambiente de ser necesario.

# ### a. Se crea la conexión y la base de datos

# Se crea la conexión
# In[11]:
address = "postgresql://postgres:sjgm1324@localhost"
engine = create_engine(address)
conn = engine.raw_connection()

try:
    cursor = conn.cursor()
    conn.commit()
    print("La conexión fue creada exitosamente!!")
    conn.close()
except:
    print("Hubo un error!")

# Se crea la base de datos "alkemy"
# In[5]:
engine = create_engine("postgresql://postgres:sjgm1324@localhost")
conn = engine.connect()
conn.execute("commit")

try:
    conn.execute("create database alkemy")
    conn.close()
    print("La base de datos 'alkemy' fue creada exitosamente!!")
except:
    print("La base de datos 'alkemy' ya existe! (o hubo un error)")


# ### b. Se crea la "tabla_01"
# In[70]:

# Se crea la conexión a la nueva base de datos
engine = create_engine("postgresql://postgres:sjgm1324@localhost/alkemy")
conn = engine.connect()
conn.execute("commit")


# Se ejecuta la query
try:
    conn.execute(
        """CREATE TABLE tabla_01 (
                index SERIAL,
                cod_localidad VARCHAR(100),
                id_provincia VARCHAR(100),
                id_departamento VARCHAR(100),
                categoría VARCHAR(100),
                provincia VARCHAR(100),
                localidad VARCHAR(100),
                nombre VARCHAR(200),
                domicilio VARCHAR(100),
                codigo_postal VARCHAR(100),
                numero_telefono VARCHAR(100),
                mail VARCHAR(200),
                web VARCHAR(200),
                fecha_carga DATE DEFAULT CURRENT_DATE
                )"""
    )
    conn.close()
    print("La 'tabla_01' fue creada exitosamente!!")
except:
    print("La 'tabla_01' ya existe! (O hubo un error)")


# ### c. Se crea la "tabla_categorias"
# In[782]:
engine = create_engine("postgresql://postgres:sjgm1324@localhost/alkemy")
conn = engine.connect()
conn.execute("commit")

# Se ejecuta la query
try:
    conn.execute(
        """CREATE TABLE tabla_categorias (
            index SERIAL,
            categoria VARCHAR(100),
            cantidad INT,
            fecha_carga DATE DEFAULT CURRENT_DATE
            )
           """
    )
    conn.close()
    print("La 'tabla_categorias' fue creada exitosamente!!")
except:
    print("La 'tabla_categorias' ya existe! (O hubo un error)")


# ### d. Se crea la "tabla_fuentes"
# In[782]:
engine = create_engine("postgresql://postgres:sjgm1324@localhost/alkemy")
conn = engine.connect()
conn.execute("commit")

# Se ejecuta la query
try:
    conn.execute(
        """CREATE TABLE tabla_fuentes (
            index SERIAL,
            fuente VARCHAR(100),
            cantidad INT,
            fecha_carga DATE DEFAULT CURRENT_DATE
            )
           """
    )
    conn.close()
    print("La 'tabla_fuentes' fue creada exitosamente!!")
except:
    print("La 'tabla_fuentes' ya existe! (O hubo un error)")


# ### e. Se crea la "tabla_cat_prov"
# In[782]:
engine = create_engine("postgresql://postgres:sjgm1324@localhost/alkemy")
conn = engine.connect()
conn.execute("commit")

# Se ejecuta la query
try:
    conn.execute(
        """CREATE TABLE tabla_cat_prov (
            index SERIAL,
            categoria VARCHAR(100),
            provincia VARCHAR(100),
            cantidad INT,
            fecha_carga DATE DEFAULT CURRENT_DATE
            )
           """
    )
    conn.close()
    print("La 'tabla_cat_prov' fue creada exitosamente!!")
except:
    print("La 'tabla_cat_prov' ya existe! (O hubo un error)")


# ### f. Se crea la "tabla_cines"
# In[782]:
engine = create_engine("postgresql://postgres:sjgm1324@localhost/alkemy")
conn = engine.connect()
conn.execute("commit")

# Se ejecuta la query
try:
    conn.execute(
        """CREATE TABLE tabla_cines (
            index SERIAL,
            Provincia VARCHAR(100),
            Butacas INT,
            Pantallas INT,
            espacio_INCAA INT,
            fecha_carga DATE DEFAULT CURRENT_DATE
            )
           """
    )
    conn.close()
    print("La 'tabla_cines' fue creada exitosamente!!")
except:
    print("La 'tabla_cines' ya existe! (O hubo un error)")


# # 4. Actualización de la base de datos:
# Luego de normalizar la información y generar las demás tablas, las mismas se
# deben actualizar en la base de datos. Para eso, es importante tener en cuenta que:
# * Todos los registros existentes deben ser reemplazados por la nueva información.
# * Dentro de cada tabla debe indicarse en una columna adicional la fecha de carga.
# * Los registros para los cuales la fuente no brinda información deben cargarse como nulos.

# ### a. tabla_01
# Se crea una columna con la fecha de carga y se exportan los datos a la tabla
# In[ ]:
df["fecha_carga"] = pd.Timestamp.now()
df["fecha_carga"] = pd.to_datetime(df["fecha_carga"], format="%Y/%m/%d")
df["fecha_carga"] = df["fecha_carga"].dt.strftime("%Y-%m-%d")

engine = create_engine("postgresql://postgres:sjgm1324@localhost/alkemy")
conn = engine.connect()
conn.execute("commit")

try:
    with engine.connect().execution_options(autocommit=True) as conn:
        df.to_sql("tabla_01", con=conn, method="multi", if_exists="replace", index=True)
    print("Los datos de la 'tabla_01' fueron exportados exitosamente!!")
except:
    print("Hubo un error!")


# ### b. tabla_categorias
# Se crea una columna con la fecha de carga y se exportan los datos a la tabla
# In[ ]:
df_countbycat["fecha_carga"] = pd.Timestamp.now()
df_countbycat["fecha_carga"] = pd.to_datetime(
    df_countbycat["fecha_carga"], format="%Y/%m/%d"
)
df_countbycat["fecha_carga"] = df_countbycat["fecha_carga"].dt.strftime("%Y-%m-%d")

try:
    with engine.connect().execution_options(autocommit=True) as conn:
        df_countbycat.to_sql(
            "tabla_categorias",
            con=conn,
            method="multi",
            if_exists="replace",
            index=True,
        )
    print("Los datos de la 'tabla_categorias' fueron exportados exitosamente!!")
except:
    print("Hubo un error!")


# ### c. tabla_fuentes
# Se crea una columna con la fecha de carga y se exportan los datos a la tabla
# In[ ]:
df_fuente["fecha_carga"] = pd.Timestamp.now()
df_fuente["fecha_carga"] = pd.to_datetime(df_fuente["fecha_carga"], format="%Y/%m/%d")
df_fuente["fecha_carga"] = df_fuente["fecha_carga"].dt.strftime("%Y-%m-%d")

try:
    with engine.connect().execution_options(autocommit=True) as conn:
        df_fuente.to_sql(
            "tabla_fuentes",
            con=conn,
            method="multi",
            if_exists="replace",
            index=True,
        )
    print("Los datos de la 'tabla_fuentes' fueron exportados exitosamente!!")
except:
    print("Hubo un error!")


# ### d. tabla_cat_prov
# Se crea una columna con la fecha de carga y se exportan los datos a la tabla
# In[ ]:
df_countbyprov["fecha_carga"] = pd.Timestamp.now()
df_countbyprov["fecha_carga"] = pd.to_datetime(
    df_countbyprov["fecha_carga"], format="%Y/%m/%d"
)
df_countbyprov["fecha_carga"] = df_countbyprov["fecha_carga"].dt.strftime("%Y-%m-%d")

try:
    with engine.connect().execution_options(autocommit=True) as conn:
        df_countbyprov.to_sql(
            "tabla_cat_prov", con=conn, method="multi", if_exists="replace", index=True
        )
    print("Los datos de la 'tabla_cat_prov' fueron exportados exitosamente!!")
except:
    print("Hubo un error!")


# ### e. tabla_cines
# Se crea una columna con la fecha de carga y se exportan los datos a la tabla
# In[ ]:
df_cines["fecha_carga"] = pd.Timestamp.now()
df_cines["fecha_carga"] = pd.to_datetime(df_cines["fecha_carga"], format="%Y/%m/%d")
df_cines["fecha_carga"] = df_cines["fecha_carga"].dt.strftime("%Y-%m-%d")

try:
    with engine.connect().execution_options(autocommit=True) as conn:
        df_cines.to_sql(
            "tabla_cines", con=conn, method="multi", if_exists="replace", index=True
        )
    conn.close()
    print(
        "Los datos de la 'tabla_cines' fueron exportados exitosamente y se cerró la conexión!!"
    )
except:
    print("Hubo un error!")

# %%
