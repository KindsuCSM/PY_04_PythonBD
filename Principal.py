import mariadb
import sqlite3
import psycopg
import psycopg_binary
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
    cursor.execute("CREATE TABLE IF NOT EXISTS CristinaSanchez (id_persona INT PRIMARY KEY, nombre CHAR(20), apellido1 CHAR(20), apellido2 CHAR(20), edad INT, fecha_nacimiento DATE, num_mascotas INT, altura FLOAT, es_estudiante BOOL);")
    conexion.commit()
    print("La tabla se ha creado correctamente")

def insertar_registros(conexion, cursor, s_id_persona, s_nombre, s_apellido1, s_apellido2, s_edad, s_fecha_nacimiento,s_num_mascotas, s_altura, s_es_estudiante):
        cursor.execute(f"INSERT INTO CristinaSanchez (id_persona, nombre, apellido1, apellido2, edad, fecha_nacimiento, num_mascotas, altura, es_estudiante) VALUES ('{s_id_persona}', '{s_nombre}', '{s_apellido1}', '{s_apellido2}', '{s_edad}', '{s_fecha_nacimiento}', '{s_num_mascotas}', '{s_altura}', '{s_es_estudiante}');")
        conexion.commit()
        print("Registro insertado con éxito")

def insertar_registro_datos_por_consola(conexion, cursor):
    numRegistros = int(input("Ingrese el número de registros que desea ingresar: "))
    for i in range(numRegistros):
        print(f"Registro: {i + 1}/{numRegistros}")
        s_id_persona = input("Id: ")
        s_nombre = input("Nombre: ")
        s_apellido1 = input("Apellido 1: ")
        s_apellido2 = input("Apellido 2: ")
        s_edad = int(input("Edad: "))
        s_fecha_nacimiento = input("Fecha nacimiento(yyyy-mm-dd): ")
        s_num_mascotas = int(input("Número de mascotas: "))
        s_altura = float(input("Altura: "))
        s_es_estudiante = str(input("Estudiante(true-false): "))
        insertar_registros(conexion, cursor, s_id_persona, s_nombre, s_apellido1, s_apellido2, s_edad, s_fecha_nacimiento, s_num_mascotas, s_altura, s_es_estudiante)

def obtener_datos_de_tabla(conexion, cursor):
    cursor.execute("SELECT * FROM CristinaSanchez;")
    conexion.commit()
    registros = cursor.fetchall()
    conexion.commit()
    print("Registros de la tabla: ")
    for registro in registros:
        print(f"\t'{registro}'")

def eliminar_tabla(conexion, cursor):
    print("Eliminando la tabla...")
    cursor.execute("DROP TABLE IF EXISTS CristinaSanchez;")
    conexion.commit()
    print("Tabla eliminada con éxito")

def eliminar_registros_consola(conexion, cursor):
    print("¿A partir de qué valor desea eliminar?")
    print("1 - Id")
    print("2 - Nombre")
    print("3 - Apellido 1")
    print("4 - Apellido 2")
    print("5 - Edad")
    print("6 - Mascotas")
    print("7 - Altura")

    valor = int(input("Ingrese el número: "))
    if valor in [1, 2, 3, 4, 5, 6, 7]:
        if valor == 1:
            id = int(input("Id: "))
            cursor.execute(f"DELETE FROM CristinaSanchez WHERE id_persona = '{id}';")
        elif valor == 2:
            nombre = input("Nombre: ")
            cursor.execute(f"DELETE FROM CristinaSanchez WHERE nombre = '{nombre}';")
        elif valor == 3:
            apellido1 = input("Apellido 1: ")
            cursor.execute(f"DELETE FROM CristinaSanchez WHERE apellido1 = '{apellido1}';")
        elif valor == 4:
            apellido2 = input("Apellido 2: ")
            cursor.execute(f"DELETE FROM CristinaSanchez WHERE apellido2 = '{apellido2}';")
        elif valor == 5:
            edad = int(input("Edad: "))
            cursor.execute(f"DELETE FROM CristinaSanchez WHERE edad = '{edad}';")
        elif valor == 6:
            num_mascotas = int(input("Número de mascotas: "))
            cursor.execute(f"DELETE FROM CristinaSanchez WHERE num_mascotas = '{num_mascotas}';")
        elif valor == 7:
            altura = float(input("Altura: "))
            cursor.execute(f"DELETE FROM CristinaSanchez WHERE altura = '{altura}';")
        conexion.commit()
    else:
        print("Valor no válido, debe ingresar un valor del 1 al 7")

    obtener_datos_de_tabla(conexion, cursor)


def eliminar_registros(conexion, cursor):
    cursor.execute(f"DELETE FROM CristinaSanchez WHERE id_persona = '1';")
    conexion.commit()
    print("Se ha eliminado correctamente la persona cuyo id = 1")

    cursor.execute(f"DELETE FROM CristinaSanchez WHERE altura = '1.80';")
    conexion.commit()
    print("Se ha eliminado correctamente la persona cuya altura = 1.80")

def crear_tabla_completa(conexion, cursor):
    nombres = ["Cristina", "Alberto", "David", "Maria", "Jesús"]
    apellidos_prim = ["Sanchez", "Giménez", "Moreno", "Velázquez", "Afonso"]
    apellidos_sec = ["González", "Rodríguez", "Fernandez", "Lucero", "Quiroga"]
    edades = [20, 60, 35, 50, 28]
    fechas_nacimiento = ['2003-05-15', '1963-11-22', '1988-03-10', '1973-07-05', '1995-09-18']
    num_mascotas = [2, 0, 5, 4, 1]
    alturas = [1.72, 1.45, 1.80, 1.90, 1.70]
    estudiantes = ['true', 'false', 'false', 'true', 'false']

    #Eliminamos la tabla si ya existe para que no nos de problemas con la primary key
    eliminar_tabla(conexion, cursor)

    #Creamos la tabla si no existe
    print("Creando la tabla...")
    crear_tabla(conexion, cursor)

    print("Insertando registros en la tabla...")
    #Insertamos los datos en la tabla
    for i in range(5):
        insertar_registros(conexion, cursor, i+1, nombres[i], apellidos_prim[i], apellidos_sec[i], edades[i], fechas_nacimiento[i], num_mascotas[i], alturas[i], estudiantes[i])

    print("Los registros que tiene la tabla actualmente: ")
    #Mostramos la tabla ya creada
    obtener_datos_de_tabla(conexion, cursor)

    print("Eliminando los registros...")
    #Eliminamos los registros ya preestablecidos en la función, en caso de querer eliminarlo manualmente iremos a la opción 5 del menú
    eliminar_registros(conexion, cursor)

    #Mostramos la tabla actualizada con los registros eliminados
    print("Los registros de la tabla se han actualizado, ahora tiene: ")
    obtener_datos_de_tabla(conexion, cursor)

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
            print("1 - Crear la tabla con los 5 registros")
            print("2 - Crear tabla")
            print("3 - Insertar registros")
            print("4 - Obtener datos")
            print("5 - Eliminar registros")
            print("6 - Eliminar tabla")
            print("0 - Volver")
            accion_recogida = input("Ingrese el número(0-6): ")
            if accion_recogida == '0':
                sigue_bbdd = False
            if accion_recogida == '1':
                crear_tabla_completa(conexion, cursor)
            elif accion_recogida == '2':
                crear_tabla(conexion, cursor)
            elif accion_recogida == '3':
                insertar_registro_datos_por_consola(conexion, cursor)
            elif accion_recogida == '4':
                obtener_datos_de_tabla(conexion, cursor)
            elif accion_recogida == '5':
                eliminar_registros_consola(conexion, cursor)
            elif accion_recogida == '6':
                eliminar_tabla(conexion, cursor)
            cursor.close()
            conexion.close()
        print("Conexión cerrada.")
