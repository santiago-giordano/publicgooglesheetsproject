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

# In[1]:


import pandas as pd
import requests
from datetime import date

today = date.today()
today = today.strftime("%d-%m-%Y")

r = requests.get(
    "https://docs.google.com/spreadsheet/ccc?key=1PS2_yAvNVEuSY0gI8Nky73TQMcx_G1i18lm--jOGfAA&output=csv"
)
open(f"D:\DATA\Alkemy\museos\\2022-octubre\museos-{today}.csv", "wb").write(r.content)
df1 = pd.read_csv(f"D:\DATA\Alkemy\museos\\2022-octubre\museos-{today}.csv")
df1["subcategoria"].fillna("Museos", inplace=True)
df1.head()


# In[2]:


df1.pivot_table(index="subcategoria", aggfunc="count", values="nombre")


# In[3]:


r = requests.get(
    "https://docs.google.com/spreadsheet/ccc?key=1o8QeMOKWm4VeZ9VecgnL8BWaOlX5kdCDkXoAph37sQM&output=csv"
)
open(f"D:\DATA\Alkemy\cines\\2022-octubre\cines-{today}.csv", "wb").write(r.content)
df2 = pd.read_csv(f"D:\DATA\Alkemy\cines\\2022-octubre\cines-{today}.csv")
df2.head()


# In[4]:


df2.pivot_table(index="Categoría", aggfunc="count", values="Nombre")


# In[5]:


r = requests.get(
    "https://docs.google.com/spreadsheet/ccc?key=1udwn61l_FZsFsEuU8CMVkvU2SpwPW3Krt1OML3cYMYk&output=csv"
)
open(f"D:\DATA\Alkemy\\bibliotecas\\2022-octubre\\bibliotecas-{today}.csv", "wb").write(
    r.content
)
df3 = pd.read_csv(f"D:\DATA\Alkemy\\bibliotecas\\2022-octubre\\bibliotecas-{today}.csv")
df3.head()


# In[6]:


df3.pivot_table(index="Categoría", aggfunc="count", values="Nombre")


# # **2. Procesamiento de los datos**
#
# Normalizar toda la información de Museos, Salas de Cine y Bibliotecas
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

# > ### A. Se normaliza el dataframe de museos:

# In[7]:


# Se eliminan las columnas innecesarias:
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


# In[8]:


# Se renombran las columnas restantes según indicaciones de normalización:
df1.rename(
    columns={
        "Cod_Loc": "cod_localidad",
        "IdProvincia": "id_provincia",
        "IdDepartamento": "id_departamento",
        "subcategoria": "categoría",
        "direccion": "domicilio",
        "CP": "codigo_postal",
        "telefono": "numero_telefono",
        "Mail": "mail",
        "Web": "web",
    },
    inplace=True,
)


# In[9]:


# Se cambian los tipos de datos del dataframe resultante:
df1 = df1.astype("string")


# In[10]:


df1.head()


# > ### B. Se normaliza el dataframe de cines:

# In[11]:


# Se eliminan las columnas innecesarias:
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


# In[12]:


# Se renombran las columnas restantes según indicaciones de normalización:
df2.rename(
    columns={
        "Cod_Loc": "cod_localidad",
        "IdProvincia": "id_provincia",
        "IdDepartamento": "id_departamento",
        "Categoría": "categoría",
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


# In[13]:


# Se cambian los tipos de datos del dataframe resultante:
df2 = df2.astype("string")


# In[14]:


df2.head()


# > ### C. Se normaliza el dataframe de bibliotecas:

# In[15]:


df3.dtypes


# In[16]:


# Se eliminan las columnas innecesarias:
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


# In[17]:


# Se renombran las columnas restantes según indicaciones de normalización:
df3.rename(
    columns={
        "Cod_Loc": "cod_localidad",
        "IdProvincia": "id_provincia",
        "IdDepartamento": "id_departamento",
        "Categoría": "categoría",
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


# In[18]:


# Se cambian los tipos de datos del dataframe resultante:
df3 = df3.astype("string")


# In[19]:


df3.head(2)


# > ### D. Se unen los tres dataframes en una única tabla:

# In[20]:


frames = [df1, df2, df3]

df = pd.concat(frames, ignore_index=True)


# In[21]:


df.head(1500)


# In[22]:


def correct(x):
    if x == "Santa Fé":
        return "Santa Fe"
    elif x == "Tierra del Fuego":
        return "Tierra del Fuego, Antártida e Islas del Atlántico Sur"
    else:
        return x


# In[23]:


df["provincia"] = df["provincia"].apply(correct)


# In[24]:


df["provincia"] = df["provincia"].astype("string")


# In[25]:


# Se exporta el dataframe a un nuevo archivo excel
df.to_excel("tabla única - museos_cines_bibliotecas.xlsx", sheet_name="Tabla única")


# In[26]:


df.dtypes


# > ### E. Procesar los datos conjuntos para poder generar una tabla con la siguiente información:
# * Cantidad de registros totales por categoría
# * Cantidad de registros totales por fuente
# * Cantidad de registros por provincia y categoría

# In[27]:


df_countbycat = df.pivot_table(index="categoría", aggfunc="count", values="nombre")


# In[28]:


df_countbycat.reset_index(inplace=True)
df_countbycat.rename(columns={"nombre": "cantidad"}, inplace=True)
df_countbycat


# In[29]:


# Se exporta el dataframe a un nuevo archivo excel
df_countbycat.to_excel(
    "registros totales por categoría.xlsx", sheet_name="RT por Categoría"
)


# In[30]:


"df_countbysc"  # Falta registros totales por fuente


# In[31]:


df_countbyprov = df.pivot_table(
    index=["categoría", "provincia"], aggfunc="count", values="nombre"
)


# In[32]:


df_countbyprov.reset_index(inplace=True)
df_countbyprov.rename(columns={"nombre": "cantidad"}, inplace=True)


# In[33]:


df_countbyprov


# In[34]:


df_countbyprov.to_excel(
    "registros por provincia y categoría.xlsx", sheet_name="Por Prov-Cat"
)


# > ### F. Procesar la información de cines para poder crear una tabla que contenga:
# * Provincia
# * Cantidad de pantallas
# * Cantidad de butacas
# * Cantidad de espacios INCAA

# In[35]:


r = requests.get(
    "https://docs.google.com/spreadsheet/ccc?key=1o8QeMOKWm4VeZ9VecgnL8BWaOlX5kdCDkXoAph37sQM&output=csv"
)
open("df_cines.csv", "wb").write(r.content)
df_cines = pd.read_csv("df_cines.csv")
df_cines.head()


# In[36]:


df_cines = df_cines[["Provincia", "Pantallas", "Butacas", "espacio_INCAA"]]
df_cines.head()


# In[37]:


# Se cambian los valores null por el string "s/d":
df_cines.fillna("0", inplace=True)


# In[38]:


df_cines.head(400)


# In[39]:


a = df_cines["espacio_INCAA"].unique()
print(sorted(a))


# In[40]:


def unique(x):
    if x == "si":
        return "1"
    elif x == "SI":
        return "1"
    else:
        return x


# In[41]:


df_cines["espacio_INCAA"] = df_cines["espacio_INCAA"].apply(unique)


# In[42]:


df_cines.head(400)


# In[43]:


df_cines["Provincia"] = df_cines["Provincia"].astype("string")
df_cines["espacio_INCAA"] = df_cines["espacio_INCAA"].astype("int64")
df_cines.dtypes


# In[44]:


# Se chequea que no haya valores duplicados en la columna "Provincia"
a = df_cines["Provincia"].unique()
print(sorted(a))


# In[45]:


df_cines = df_cines.pivot_table(
    index="Provincia", aggfunc="sum", values=["Pantallas", "Butacas", "espacio_INCAA"]
)


# In[46]:
df_cines.head()


# In[46]:


# Se resetea el index del dataframe df_cines para incluir "Provincia" como columna
df_cines.reset_index(inplace=True)
df_cines.dtypes


# In[47]:


df_cines


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

# In[11]:


# Se importa el módulo, se establece la conexión y se crea la base de datos
from sqlalchemy import create_engine

address = "postgresql://postgres:sjgm1324@localhost"
engine = create_engine(address)
conn = engine.raw_connection()
cursor = conn.cursor()
conn.commit()
print("La conexión fue creada exitosamente!!")
conn.close()


# In[5]:


from sqlalchemy import create_engine

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

# ### n. Se crea la "tabla_cines"

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
    print("Los datos fueron exportados exitosamente!!")
except:
    print("Hubo un error!")

# ### b. tabla_cines
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
    print("Los datos fueron exportados exitosamente!!")
except:
    print("Hubo un error!")

# In[ ]:


# In[ ]:


# In[ ]:


# In[ ]:
