import psycopg2
from pymongo import MongoClient
from config import DBNAME, HOST, USER, PASSWORD, PORT, MONGO_HOST, MONGO_PORT

class PostgresAdapter:
    def __init__(self):
        self.__connection = None
        self.__cursor = None


    def connect(self):
        try:
            self.__connection = psycopg2.connect(dbname=DBNAME, user=USER, password=PASSWORD, host=HOST, port=PORT)
            self.__cursor = self.__connection.cursor()
        except psycopg2.Error as e:
            print("Ошибка подключения", e)


    def fetch_data(self, query, params = None):
        if self.__cursor is None:
            return None

        try:
            self.__cursor.execute(query, params)
            return self.__cursor.fetchall()
        except psycopg2.Error as e:
            print("Ошибка извлечения данных", e)


    def disconnect(self):
        self.__cursor.close()
        self.__connection.close()


def mongo_conn():
    try:
        db = MongoClient(MONGO_HOST, MONGO_PORT).sets
    except Exception as e:
        print("Ошибка подключения к MongoDB", e)
        db = None
    return db