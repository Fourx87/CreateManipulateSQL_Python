import mysql.connector
from mysql.connector import Error

# Criando um novo Banco de Dados

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f'Error: {err}')