import mysql.connector
from mysql.connector import Error
import pandas as pd
from createMySQL_connection import *
from createDataBase import *
from execucaoConsultasDB import *


# Criando conexão com MySQL
connection = Create_db_connection("localhost", "root", "12345", "school")

# Consulta para criar banco de dados
create_database_query = "CREATE DATABASE school"
create_database(connection, create_database_query)

# Criando a tabela professores
create_teacher_table = '''
CREATE TABLE teacher (
    teacher_id INT PRIMARY KEY,
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(40) NOT NULL,
    language_1 VARCHAR(3) NOT NULL,
    language_2 VARCHAR(3),
    dob DATE,
    tax_id INT UNIQUE,
    phone_no VARCHAR(20) 
    );
'''

# Criando tabela Clientes
create_client_table = '''
CREATE TABLE client (
    client_id INT PRIMARY KEY,
    client_name VARCHAR(40) NOT NULL,
    adress VARCHAR(60) NOT NULL,
    phone_no VARCHAR(20)
    )
'''

# Criando tabela participantes
create_participant_table = '''
CREATE TABLE participant (
    participant_id INT PRIMARY KEY,
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(40) NOT NULL,
    phone_no VARCHAR(20),
    client INT
    )
'''

# Criando tabela cursos
create_course_table = '''
CREATE TABLE course (
    course_id INT PRIMARY KEY,
    course_name VARCHAR(40) NOT NULL,
    language VARCHAR(3) NOT NULL,
    level VARCHAR(2),
    course_length_weeks INT,
    start_date DATE,
    in_school BOOLEAN,
    teacher INT,
    client INT
    )
'''
connection = Create_db_connection('localhost', 'root', '12345', 'school')
execute_query(connection, create_teacher_table)
execute_query(connection, create_client_table)
execute_query(connection, create_participant_table)
execute_query(connection, create_course_table)