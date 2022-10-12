CREATE TABLE tabla_01 (
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
                );
				
CREATE TABLE tabla_categorias (
            index SERIAL,
            categoria VARCHAR(100),
            cantidad INT,
            fecha_carga DATE DEFAULT CURRENT_DATE
            );
			
CREATE TABLE tabla_fuentes (
            index SERIAL,
            fuente VARCHAR(100),
            cantidad INT,
            fecha_carga DATE DEFAULT CURRENT_DATE
            );

CREATE TABLE tabla_cat_prov (
            index SERIAL,
            categoria VARCHAR(100),
            provincia VARCHAR(100),
            cantidad INT,
            fecha_carga DATE DEFAULT CURRENT_DATE
            );

CREATE TABLE tabla_cines (
            index SERIAL,
            Provincia VARCHAR(100),
            Butacas INT,
            Pantallas INT,
            espacio_INCAA INT,
            fecha_carga DATE DEFAULT CURRENT_DATE
            );