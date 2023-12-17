from pprint import pprint
import psycopg2
from psycopg2.sql import SQL, Identifier

def create_db(conn):
    curs = conn.cursor()
    curs.execute("""
        DROP TABLE IF EXISTS Phone;
        DROP TABLE IF EXISTS Client;
    """)
    curs.execute("""
        CREATE TABLE Client(
        id SERIAL PRIMARY KEY,
        name VARCHAR(60) NOT NULL,
        surname VARCHAR(60) NOT NULL,
        email VARCHAR(60) NOT NULL
        );
    """)
    curs.execute("""
        CREATE TABLE Phone(
        Phone_id SERIAL PRIMARY KEY,
        Client_id INTEGER REFERENCES Client(id),
        Phone_Number VARCHAR(64) NOT NULL
        );
    """)
    conn.commit()

def add_client(conn, name: str, surname: str, email: str):
    curs = conn.cursor()
    curs.execute("""
    INSERT INTO Client(name,surname,email) VALUES(%s,%s,%s);
    """, (name, surname, email))
    conn.commit()

def add_phone(conn,client_id,phone):
    curs = conn.cursor()
    curs.execute("""
    INSERT INTO Phone(Client_id,Phone_Number) VALUES(%s,%s);
    """, (client_id, phone))
    conn.commit()

def change_client(conn,client_id,name=None, surname=None, email=None):
    curs = conn.cursor()

    arg_list = {'name': name, 'surname': surname, 'email': email}
    for key, arg in arg_list.items():
        if arg:
            curs.execute(SQL("UPDATE Client SET {}=%s WHERE id=%s").format(Identifier(key)), (arg, client_id)) #Identifier что-то неизвестное но это работает
    conn.commit()

def delete_phone(conn,client_id,phone):
    curs = conn.cursor()
    curs.execute("""
            DELETE FROM Phone WHERE Client_id = %s and Phone_Number = %s;
            """, (client_id,phone))
    conn.commit()

def delete_client(conn, client_id):
    curs = conn.cursor()
    curs.execute("""
                DELETE FROM Phone WHERE client_id = %s;
                """, (client_id,))
    curs.execute("""
            DELETE FROM Client WHERE id = %s;
            """, (client_id,))
    conn.commit()

def find_client(conn,name=None, surname=None, email=None, phone=None) -> int:
    curs = conn.cursor()
    curs.execute("""
                SELECT client.id from Client LEFT JOIN Phone ON client.id = client_id 
                WHERE name = %s OR surname = %s OR email = %s OR Phone_Number = %s;
            """,(name,surname,email,phone))
    found_client = curs.fetchone()
    if found_client is None:
        print("Таких клиентов нет")
    else:
        print('Id найденного клиента: ',found_client[0])

with psycopg2.connect(database="clients",user="postgres",password="201224") as conn:
    with conn.cursor() as cur:
        create_db(conn)

        add_client(conn, 'Vova', 'Derevyagin', 'Vova@gmail.com')
        add_client(conn, 'Vlad', 'Derevyagin', 'Vlad@gmail.com')
        add_client(conn, 'Oleg', 'Ivanov', 'OIvanov@yandex.ru')

        cur.execute("""
            SELECT * from Client;
        """)
        print("Добавляем клиентов в БД")
        pprint(cur.fetchall())

        add_phone(conn, 1, '+79213376080')
        add_phone(conn, 1, '+79213374060')
        add_phone(conn, 2, '56560')
        add_phone(conn, 3, '+89213553411')

        cur.execute("""
            SELECT * from Phone;
        """)
        print("\nДобавляем номера телефонов клиентов в БД")
        pprint(cur.fetchall())

        change_client(conn,1,'Vladimir')
        change_client(conn,3,email="OlegIvanov@yande.ru")

        cur.execute("""
            SELECT * from Client;
        """)
        print("\nМеняем данные клиентов в БД")
        pprint(cur.fetchall())

        delete_phone(conn, 1, '+79213376080')
        delete_phone(conn,2,'56560')

        cur.execute("""
            SELECT * from Phone;
        """)
        print("\nУдаляем номера телефонов клиентов в БД")
        pprint(cur.fetchall(), width=40)



        delete_client(conn,2)
        delete_client(conn,1)

        cur.execute("""
            SELECT * from Client;
        """)
        print("\nУдаляем клиентов из БД")
        pprint(cur.fetchall())

        find_client(conn,'Vladimir')
        find_client(conn,phone='+89213553411')
