import mariadb
import sqlite3
import psycopg
import sys

user = 'root'
password = ''
host = 'localhost'
port = 3306
database = 'bdpython'

#Preguntar al usuario a que base de datos desea conectarse
def preguntar_bd():
    while True:
        print("¿Qué base de datos desea usar?")
        print("1 - PostgreSQL")
        print("2 - MariaDB")
        print("3 - SQLite3")
        print("0 - Salir")
        respuesta = int(input("Ingrese la opción(0-3): "))
        if respuesta in [0, 1, 2, 3]:
            return respuesta
        else:
            print("Por favor, ingrese una opción válida (0-3).")
#Conectar a la base de datos que haya introducido el usuario
def conectar_bd(respuesta):
    conexion = None
    try:
        if respuesta == 1:
            #Nos conectamos a la base de datos con los datos necesarios para PostgreSQL
            conexion = psycopg.connect(
                host=host,
                user='postgres',
                port=5432,
                password='usuario',
                dbname = database
            )
            print(f"La conexión con PostgreSQL  se ha realizado con éxito")
        elif respuesta == 2:
            #Nos conectamos a la base de datos con los datos necesarios para MariaDB
            conexion = mariadb.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port
            )
            print(f"La conexión con MariaDB se ha realizado con éxito")

        elif respuesta == 3:
            # En SQLite nos conectamos directamente al archivo
            conexion = sqlite3.connect(database)
            print(f"La conexión con SQLite se ha realizado con éxito")
        cursor = conexion.cursor()

        # Retornamos la conexion para poder hacer commit de todos los cambios que hagamos en la tabla
        # ademas del cursor para poder ejecutar las acciones
        return conexion, cursor

    except Exception as e:
        print(f"Error al conectar con una base de datos: {e}")
        sys.exit(1)

def crear_tabla(conexion, cursor):
    # Aqui primero eliminaba la tabla pero es mas eficaz crear la tabla SI NO existe
    # cursor.execute("DROP TABLE IF EXISTS CristinaSanchez;")
    cursor.execute("CREATE TABLE IF NOT EXISTS CristinaSanchez (id_persona INT PRIMARY KEY, nombre CHAR(20), apellido1 CHAR(20), apellido2 CHAR(20), edad INT, num_mascotas INT, altura FLOAT);")
    conexion.commit()
    print("La tabla se ha creado correctamente")

def insertar_registros(conexion, cursor):

    numRegistros = int(input("Ingrese el número de registros que desea ingresar: "))

    for i in range(numRegistros):
        print(f"Registro: {i+1}/{numRegistros}")
        s_id_persona = input("Id: ")
        s_nombre = input("Nombre: ")
        s_apellido1 = input("Apellido 1: ")
        s_apellido2 = input("Apellido 2: ")
        s_edad = int(input("Edad: "))
        s_num_mascotas = int(input("Número de mascotas: "))
        s_altura = float(input("Altura: "))

        cursor.execute(f"INSERT INTO CristinaSanchez (id_persona, nombre, apellido1, apellido2, edad, num_mascotas, altura) VALUES ('{s_id_persona}', '{s_nombre}', '{s_apellido1}', '{s_apellido2}', '{s_edad}', '{s_num_mascotas}', '{s_altura}');")
        conexion.commit()
        print("Registro insertado con éxito")

def obtener_datos(conexion, cursor):
    cursor.execute("SELECT * FROM CristinaSanchez;")
    conexion.commit()
    registros = cursor.fetchall()
    conexion.commit()
    print("Registros de la tabla: ")
    for registro in registros:
        print(f"\t'{registro}'")

def eliminar_tabla(conexion, cursor):
    cursor.execute("DROP TABLE IF EXISTS CristinaSanchez;")
    conexion.commit()
    print("Tabla eliminada con éxito")

def eliminar_registros(conexion, cursor):
    print("¿A partir de qué valor desea eliminar?")
    print("1 - Id")
    print("2 - Nombre")
    print("3 - Apellido 1")
    print("4 - Apellido 2")
    print("5 - Edad")
    print("6 - Mascotas")
    print("7 - Altura")

    valor = input("Ingrese el número: ")
    if valor in [1, 2, 3, 4, 5, 6, 7]:
        if valor == '1':
            id = input("Id: ")
            cursor.execute(f"DELETE FROM CristinaSanchez WHERE nombre = '{id}';")
        elif valor == '2':
            nombre = input("Nombre: ")
            cursor.execute(f"DELETE FROM CristinaSanchez WHERE nombre = '{nombre}';")
        elif valor == '3':
            apellido1 = input("Apellido 1: ")
            cursor.execute(f"DELETE FROM CristinaSanchez WHERE apellido1 = '{apellido1}';")
        elif valor == '4':
            apellido2 = input("Apellido 2: ")
            cursor.execute(f"DELETE FROM CristinaSanchez WHERE apellido2 = '{apellido2}';")
        elif valor == '5':
            edad = int(input("Edad: "))
            cursor.execute(f"DELETE FROM CristinaSanchez WHERE edad = '{edad}';")
        elif valor == '6':
            num_mascotas = int(input("Número de mascotas: "))
            cursor.execute(f"DELETE FROM CristinaSanchez WHERE num_mascotas = '{num_mascotas}';")
        elif valor == '7':
            altura = float(input("Altura: "))
            cursor.execute(f"DELETE FROM CristinaSanchez WHERE altura = '{altura}';")
        conexion.commit()
    else:
        print("Valor no válido, debe ingresar un valor del 1 al 7")

    obtener_datos(conexion, cursor)

if __name__ == '__main__':
    sigue_conectar_bbdd = True

    while sigue_conectar_bbdd:
        respuesta = preguntar_bd()
        if respuesta == 0:
            print("El programa ha finalizado con éxito. ")
            break
        sigue_bbdd = True
        while sigue_bbdd:
            conexion, cursor = conectar_bd(respuesta)
            print("¿Qué desea realizar?")
            print("1 - Crear tabla")
            print("2 - Insertar registros")
            print("3 - Obtener datos")
            print("4 - Eliminar registros")
            print("5 - Eliminar tabla")
            print("0 - Volver")
            accion_recogida = input("Ingrese el número(0-5): ")
            if accion_recogida == '0':
                sigue_bbdd = False
            if accion_recogida == '1':
                crear_tabla(conexion, cursor)
            elif accion_recogida == '2':
                insertar_registros(conexion, cursor)
            elif accion_recogida == '3':
                obtener_datos(conexion, cursor)
            elif accion_recogida == '4':
                eliminar_registros(conexion, cursor)
            elif accion_recogida == '5':
                eliminar_tabla(conexion, cursor)
            cursor.close()
            conexion.close()
            print("Conexión cerrada.")
