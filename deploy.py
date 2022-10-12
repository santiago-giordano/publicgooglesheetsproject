#1. Para instalar virtualenv 
pip install virtualenv

#2. Para crear el entorno virtual "alkemy_env"
python -m venv alkemy_env 

#3. Para activar el entorno virtual "alkemy_env"
python\Scripts\activate

#4. En caso de que el módulo pip no esté instalado en el entorno virtual ("No module named pip")
python -m ensurepip 

#5. Para instalar los paquetes en el entorno virtual creado
pip install requests
pip install datetime
pip install openpyxl
pip install pandas
pip install sqlalchemy
pip install python-decouple

#6. Para ejecutar el archivo <AlkemyChallenge4.3.py>
python AlkemyChallenge4.3.py