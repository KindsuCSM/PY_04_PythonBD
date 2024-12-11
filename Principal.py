#PostgreSQL, MySQL (o MaríaDB) y SQLite3

import sqlite3
import mariadb
import psycopg
import psycopg_binary
import sys

user = 'root'
password = ''
host = 'localhost'
port = 3306
database = 'bdpython'

def preguntar_bd():
    print("¿Qué base de datos desea usar?")
    print("1 - PostgreSQL")
    print("2 - MariaDB")
    print("3 - SQLite3")
    respuesta = int(input("Ingrese la opción: "))
    return respuesta

def conectar_bd(respuesta):
    conexion = None
    try:
        if respuesta == 1:
            #Nos conectamos a la base de datos sin especificar el nombre, en caso de que no exista la creamos
            conexion = psycopg_binary.connect(
                host=host,
                user=user,
                password=password
            )
            conexion.autocommit = True
            cursor = conexion.cursor()
            cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{database}'")
            if cursor.fetchone() is None:
                print(f"La base de datos {database} no existe. Se creará")
                cursor.execute(f"CREATE DATABASE {database}")
                print("Base de datos creada. ")
            cursor.close()

            # Ahora nos conectamos a la base de datos
            conexion.close()
            conexion = psycopg_binary.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )
            print(f"La conexión con PostgreSQL se ha realizado con éxito")

        elif respuesta == 2:
            #Nos conectamos a la base de datos sin especificar el nombre, en caso de que no exista la creamos
            conexion = mariadb.connect(
                host=host,
                user=user,
                password=password,
                port=port
            )
            conexion.autocommit = True  # Necesario para crear la base de datos

            cursor = conexion.cursor()
            cursor.execute(f"SHOW DATABASES LIKE '{database}'")
            if cursor.fetchone() is None:
                print(f"La base de datos {database} no existe. Creándola...")
                cursor.execute(f"CREATE DATABASE {database}")
            cursor.close()

            # Ahora nos conectamos a la base de datos
            conexion.close()
            conexion = mariadb.connect(
                host=host,
                database='bdpython',
                user=user,
                password=password,
                port=port
            )
            print(f"La conexión con MariaDB a la base de datos {database} se ha realizado con éxito")

        elif respuesta == 3:
            # En SQLite nos conectamos directamente al archivo
            conexion = sqlite3.connect(f"{database}.db")
            print(f"La conexión con SQLite a la base de datos {database} se ha realizado con éxito")

        cursor = conexion.cursor()
        return conexion, cursor

    except Exception as e:
        print(f"Error al conectar con una base de datos: {e}")
        sys.exit(1)

def crear_tabla(cursor):
    cursor.execute("DROP TABLE IF EXISTS CristinaSanchez")

    cursor.execute("CREATE TABLE CristinaSanchez(nombre CHAR(20) PRIMARY KEY, apellido1 CHAR(20), apellido2 CHAR(20), edad INT, num_mascotas INT, altura FLOAT )")
    print("La tabla se ha creado correctamente")

def insertar_registros(cursor):
    numRegistros = int(input("Ingrese el número de registros que desea ingresar: "))

    for i in range(numRegistros):
        print(f"Registro: {i+1}/{numRegistros}")
        s_nombre = input("Nombre del registro: ")
        s_apellido1 = input("Apellido 1 del registro: ")
        s_apellido2 = input("Apellido 2 del registro: ")
        s_edad = int(input("Edad del registro: "))
        s_num_mascotas = int(input("Número de mascotas del registro: "))
        s_altura = float(input("Altura del registro: "))

        cursor.execute(f"INSERT INTO CristinaSanchez (nombre, apellido1, apellido2, edad, num_mascotas, altura) VALUES ('{s_nombre}', '{s_apellido1}', '{s_apellido2}', '{s_edad}', '{s_num_mascotas}', '{s_altura}')")
        print("Registro insertado con éxito")

def obtener_datos(cursor):
    cursor.execute("SELECT * FROM CristinaSanchez")
    registros = cursor.fetchall()
    for registro in registros:
        print(registro)

def eliminar_registros(cursor):
    print("¿A partir de qué valor desea eliminar?")
    print("1 - Nombre")
    print("2 - Apellido 1")
    print("3 - Apellido 2")
    print("4 - Edad")
    print("5 - Mascotas")
    print("6 - Altura")

    valor = input("Ingrese el número: ")

    if valor == '1':
        nombre = input("Nombre: ")
        cursor.execute(f"DELETE FROM CristinaSanchez WHERE nombre = '{nombre}'")
    elif valor == '2':
        apellido1 = input("Apellido 1: ")
        cursor.execute(f"DELETE FROM CristinaSanchez WHERE apellido1 = '{apellido1}'")
    elif valor == '3':
        apellido2 = input("Apellido 2: ")
        cursor.execute(f"DELETE FROM CristinaSanchez WHERE apellido2 = '{apellido2}'")
    elif valor == '4':
        edad = int(input("Edad: "))
        cursor.execute(f"DELETE FROM CristinaSanchez WHERE edad = {edad}")
    elif valor == '5':
        num_mascotas = int(input("Número de mascotas: "))
        cursor.execute(f"DELETE FROM CristinaSanchez WHERE num_mascotas = {num_mascotas}")
    elif valor == '6':
        altura = float(input("Altura: "))
        cursor.execute(f"DELETE FROM CristinaSanchez WHERE altura = {altura}")

    obtener_datos(cursor)

if __name__ == '__main__':
    respuesta = preguntar_bd()
    conexion, cursor = conectar_bd(respuesta)
    crear_tabla(cursor)

    print("¿Qué desea realizar?")
    print("1 - Insertar registros")
    print("2 - Obtener datos")
    print("3 - Eliminar registros")
    accion_recogida = input("Ingrese el número: ")

    if accion_recogida == '1':
        insertar_registros(cursor)
    elif accion_recogida == '2':
        obtener_datos(cursor)
    elif accion_recogida == '3':
        eliminar_registros(cursor)


    conexion.commit()

    # Cerrar conexión
    cursor.close()
    conexion.close()
    print("Conexión cerrada.")





