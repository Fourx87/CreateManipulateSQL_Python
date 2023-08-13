import mysql.connector
from mysql.connector import Error
import pandas as pd
from createMySQL_connection import *
from createDataBase import *
from execucaoConsultasDB import *



# Criando conexão com MySQL
connection = create_db_connection("localhost", "root", "Arthur12*", "school")

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
connection = create_db_connection('localhost', 'root', 'Arthur12*', 'school')
execute_query(connection, create_teacher_table)
execute_query(connection, create_client_table)
execute_query(connection, create_participant_table)
execute_query(connection, create_course_table)

# Definir relacionamento entre tabelas
alter_participant = '''
ALTER TABLE participant
ADD FOREIGN KEY(client)
REFERENCES client(client_id)
ON DELETE SET NULL;
'''

alter_course = '''
ALTER TABLE course
ADD FOREIGN KEY(teacher)
REFERENCES client(teacher_id)
ON DELETE SET NULL;
'''

alter_course_again = '''
ALTER TABLE participant
ADD FOREIGN KEY(client)
REFERENCES client(client_id)
ON DELETE SET NULL;
'''

create_takecourse_table = '''
CREATE TABLE take_course (
    participant_id INT,
    course_id INT,
    PRIMARY KEY(participant_id, course_id),
    FOREIGN KEY(participant_id) REFERENCES participant(participant_id) ON DELETE CASCADE,
    FOREIGN KEY(course_id) REFERENCES course(course_id) ON DELETE CASCADE
);
'''

connection = create_db_connection("localhost", "root", "Arthur12*", "school")
execute_query(connection, alter_participant)
execute_query(connection, alter_course)
execute_query(connection, alter_course_again)
execute_query(connection, create_takecourse_table)

# Adicionando registros na teacher_table
pop_teacher = '''
INSERT INTO teacher VALUES
(1, 'Celia', 'Maria', 'ENG', NULL, '1985-04-20',12345 , '+558374553676'),
(2, 'Sarah', 'Silva', 'FRA', NULL, '1970-02-17', 253456, '+558334567890'),
(3, 'Arthur', 'Andrade', 'MAN', 'ENG', '1990-11-12', 34567, '+558349921333'),
(4, 'Marcos', 'Amorin', 'DEU', 'ITA', '1987-07-07', 45678, '+558374553676'),
(5, 'Victor', 'Boschnaquian', 'RUS', 'ENG', '1963-05-30', 56789, '+558372635467'),
(6, 'Camila', 'Onofre', 'ENG', 'IRI', '1995-08-09', 67890, '+558331231232');
'''

connection = create_db_connection("localhost", "root", "Arthur12*", "school")
execute_query(connection, pop_teacher)

# Preencher tabela Client
pop_client = '''
INSERT INTO client VALUES
(101, 'Big Business Federation', '123 Falschungstrabe, 10999 Berlin', 'NGO'),
(102, 'eCommerce GmbH', '27 Ersatz Allee, 10317 Berlin', 'Retail'),
(103, 'AutoMaker AG', '20 Kunstlichstrabe, 10023 Berlin', 'Auto'),
(104, 'Banko Bank', '12 Betrugstrabe, 12345 Berlin', 'Banking'),
(105, 'WeMovelt GmbH', '138 arglistweg, 10065 Berlin', 'Logistics');
'''

# Preencher Tabela participant
pop_participant = '''
INSERT INTO participant VALUES
(101, 'Marina', 'Berg', '491635558182', 101),
(102, 'Andrea', 'Duerr', '49159555740', 101),
(103, 'Philipp', 'Probst', '49155555692', 102),
(104, 'Rene', 'Brandt', '4916355546', 102),
(105, 'Susanne', 'Shuster', '49155555779', 102),
(106, 'Christian', 'Schreiner', '4916255375', 101),
(107, 'Harry', 'Kim', '49177555633', 101),
(108, 'Jan', 'Nowak', '19151555824', 101),
(109, 'Pablo', 'Garcia', '49162555176', 101),
(110, 'Melanie', 'Dreschler', '49151555527', 103),
(111, 'Dieter', 'Durr', '49178555311', 103),
(112, 'Max', 'Mustermann', '49152555195', 104),
(113, 'Maxine', 'Mustermann', '49177555355', 104),
(114, 'Heiko', 'Fleischer', '49155555581', 105);
'''

# Preencher Tabela course
pop_course = '''
INSERT INTO course VALUES
(12, 'English for Logistic', 'ENG', 'A1', 10, '2020-02-01', TRUE, 1, 105),
(13, 'Beginner English', 'ENG', 'A2', 40, '2019-11-12', FALSE, 6, 101),
(14, 'Intermediate English', 'ENG', 'B2', 40, '2019-11-12', FALSE, 6, 101),
(15, 'Advanced English', 'ENG', 'C1', 40, '2019-11-12', FALSE, 6, 101),
(16, 'Mandarin Fur Autoindustrie', 'MAN', 'B1', 15, '2020-01-15', TRUE, 3, 103),
(17, 'Français Intermediaire', 'FRA', 'B1', 18, '2020-04-03', FALSE, 2, 101),
(18, 'Deutsch fur Anfanger', 'DEU', 'A2', 8, '2020-02-14', TRUE, 4, 102),
(19, 'Intermediate English', 'ENG', 'B2', 10, '2020-03-29', FALSE, 1, 104),
(20, 'Fortgestchrittenes Russisch', 'RUS', 'C1', 4, '2020-04-08', FALSE, 5, 103);
'''

# Preencher tabela take_course
pop_takecourse = '''
INSERT INTO take_course VALUES
(101, 15),
(101, 17),
(102, 17),
(103, 18),
(104, 18),
(105, 18),
(106, 13),
(107, 13),
(108, 13),
(109, 14),
(109, 15),
(110, 16),
(110, 20),
(111, 16),
(114, 12),
(112, 19),
(113, 19);
'''

connection = create_db_connection("localhost", "root", "Arthur12*", "school")
execute_query(connection, pop_client)
execute_query(connection, pop_participant)
execute_query(connection, pop_course)
execute_query(connection, pop_takecourse)

# Consulta simples
q1 = '''
SELECT *
FROM TEACHER;
'''

connection = create_db_connection("localhost", "root", "Arthur12*", "school")
results = read_query(connection, q1)

'''line()
for result in results:
    print(result)
line()'''

#Inicializa uma lista vazia
from_db_list_simple = []

# Percorrer os resultados e inseri-los à lista
# Retorna uma lista de tuplas
for result in results:
  result = list(result)
  from_db_list_simple.append(result)
line()
print(from_db_list_simple)
line()

'''# Retornar uma lista de listas  e criar um Dataframe Pandas
columns = ["course_id", "course_name", "language", "client_name", "adress"]
df = pd.DataFrame(from_db_list_simple, columns=columns)

display(df)'''

# Consulta Complexa
q5 = '''
SELECT course.course_id, course.course_name, course.language, client.client_name, client.adress
FROM course
JOIN client
ON course.client = client.client_id
WHERE course.in_school = FALSE;
'''

connection = create_db_connection("localhost", "root", "Arthur12*", "school")
results = read_query(connection, q5)

'''line()
for result in results:
    print(result)
line()'''

#Inicializa uma lista vazia
from_db_list_complex = []

# Percorrer os resultados e inseri-los à lista
# Retorna uma lista de tuplas
for result in results:
  result = list(result)
  from_db_list_complex.append(result)
line()
print(from_db_list_complex)
line()

'''# Retornar uma lista de listas  e criar um Dataframe Pandas
columns = ["course_id", "course_name", "language", "client_name", "adress"]
df = pd.DataFrame(from_db_list_complex, columns=columns)

display(df)'''

# Atualizando cadastro
update = """
UPDATE client 
SET adress = '23 Fingiertweg, 14534 Berlin' 
WHERE client_id = 101;
"""

connection = create_db_connection("localhost", "root", "Arthur12*", "school")
execute_query(connection, update)

# Deletando um cadastro (curso 20 RUS) e consultando logo após
delete_course = """
DELETE FROM course 
WHERE course_id = 20;
"""

connection = create_db_connection("localhost", "root", "Arthur12*", "school")
execute_query(connection, delete_course)
# Consulta simples Course
q1_course = '''
SELECT *
FROM COURSE;
'''

connection = create_db_connection("localhost", "root", "Arthur12*", "school")
results = read_query(connection, q1_course)

line()
for result in results:
    print(result)
line()

# Adicionar novos professores
sql = '''
    INSERT INTO teacher (teacher_id, first_name, last_name, language_1, language_2, dob, tax_id, phone_no) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    '''

val = [
    (7, 'Hank', 'Dodson', 'ENG', None, '1991-12-23', 11111, '+491772345678'),
    (8, 'Sue', 'Perkins', 'MAN', 'ENG', '1976-02-02', 22222, '+491443456432')
]

connection = create_db_connection("localhost", "root", "Arthur12*", "school")
execute_list_query(connection, sql, val)

# Verificando os novos professores
# Consulta simples
q1 = '''
SELECT *
FROM TEACHER;
'''

connection = create_db_connection("localhost", "root", "Arthur12*", "school")
results = read_query(connection, q1)

line()
for result in results:
    print(result)
line()