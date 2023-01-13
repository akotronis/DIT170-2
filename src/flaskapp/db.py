import os
import mysql.connector
from neo4j import GraphDatabase
from pymongo import MongoClient

from flaskapp import printm


def connection_established(db):
    '''A decorator for database connector methods to make sure we are connected before execute queries'''
    def wrapper(method):
        def _method(*args, **kwags):
            inst, *_ = args
            assert inst.cnx is not None, f'{db}: Not Connected!'
            return method(*args, **kwags)
        return _method
    return wrapper


class MySQLConnector:
    def __init__(self, user, password, host, database):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.cnx  = None
        try:
            self.cnx = mysql.connector.connect(user=self.user, password=self.password, database=self.database, host=self.host)
            print('MySQL: Connected!')
        except Exception as e:
            printm(e, "MySQL: Connection Failed")
    
    def close(self):
        if self.cnx is not None:
            self.cnx.close()
            self.cnx = None

    @connection_established('MySQL')
    def create_table(self, table_name: str, query: str) -> None:
        '''Create a MySQL table'''
        cursor = self.cnx.cursor()
        try:
            print(f"MySQL: Creating table: `{table_name}`")
            cursor.execute(query)
        except mysql.connector.Error as e:
            printm(e, 'MySQL: Error')
            self.cnx.rollback()
        else:
            self.cnx.commit()
            print(f'MySQL: Table `{table_name}` in place')
        finally:
            cursor.close()

    @connection_established('MySQL')
    def drop_table(self, table_name: str) -> None:
        '''Drop a MySQL table if it exists'''
        cursor = self.cnx.cursor()
        try:
            print(f"MySQL: Dropping table: `{table_name}`")
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        except mysql.connector.Error as e:
            printm(e, 'MySQL: Error')
            self.cnx.rollback()
        else:
            self.cnx.commit()
        finally:
            cursor.close()

    @connection_established('MySQL')
    def insert_user_data(self, user_data: dict) -> None:
        '''Insert user data to MySQL database table'''
        cursor = self.cnx.cursor()
        query = '''INSERT INTO users (id, name) VALUES (%(id)s, %(name)s)'''
        try:
            cursor.execute(query, user_data)
        except mysql.connector.IntegrityError as err:
            printm(title=f'MySQL: Users | Error: {err}', width=100)
            self.cnx.rollback()
        else:
            self.cnx.commit()
        finally:
            cursor.close()

    @connection_established('MySQL')
    def insert_category_data(self, category: str) -> int:
        '''Insert category data to MySQL database table'''
        cursor = self.cnx.cursor()
        query1 = '''INSERT INTO categories (title) VALUES (%s)'''
        query2 = '''SELECT id FROM categories WHERE title=%s'''
        try:
            cursor.execute(query1, (category, ))
        except mysql.connector.IntegrityError as err:
            printm(title=f'MySQL: Categories | Error: {err}', width=100)
            self.cnx.rollback()
        else:
            self.cnx.commit()
        finally:
            cursor.execute(query2, (category, ))
            category_id = cursor.fetchone()[0]
            cursor.close()
        return category_id

    @connection_established('MySQL')
    def insert_product_data(self, product_data: dict) -> None:
        '''Insert product data to MySQL database table'''
        cursor = self.cnx.cursor()
        query = '''INSERT INTO products (id, title, description, category_id)
            VALUES (%(id)s, %(title)s, %(description)s, %(category_id)s)'''
        try:
            cursor.execute(query, product_data)
        except mysql.connector.IntegrityError as err:
            printm(title=f'MySQL: Products | Error: {err}', width=100)
            self.cnx.rollback()
        else:
            self.cnx.commit()
        finally:
            cursor.close()

    @connection_established('MySQL')
    def insert_transaction_data(self, transaction_data: dict) -> None:
        '''Insert transaction data to MySQL database table'''
        cursor = self.cnx.cursor()
        query = '''INSERT INTO transactions (user_id, product_id, timestamp)
            VALUES (%(user_id)s, %(product_id)s, %(timestamp)s)'''
        try:
            cursor.execute(query, transaction_data)
        except mysql.connector.IntegrityError as err:
            printm(title=f'MySQL: Transactions | Error: {err}', width=100)
            self.cnx.rollback()
        else:
            self.cnx.commit()
        finally:
            cursor.close()


class MongoDBConnector:
    def __init__(self, uri):
        self.uri = uri
        self.cnx  = None
        try:
            self.cnx = MongoClient(host=uri)
            print('MongoDB: Connected!')
        except Exception as e:
            printm(e, "MongoDB: Connection Failed")
    
    def close(self):
        if self.cnx is not None:
            self.cnx.close()
            self.cnx = None


class Neo4jConnector:
    def __init__(self, uri, user, pwd):
        self.uri = uri
        self.user = user
        self.pwd = pwd
        self.cnx = None
        try:
            self.cnx = GraphDatabase.driver(self.uri, auth=(self.user, self.pwd))
            print('Neo4j: Connected!')
        except Exception as e:
            printm(e, 'Neo4j: Connection Failed')
        
    def close(self):
        if self.cnx is not None:
            self.cnx.close()
            self.cnx = None
    
    @connection_established('Neo4j')
    def query(self, query, parameters=None, db=None):
        session, response = None, None
        try: 
            session = self.cnx.session(database=db) if db is not None else self.cnx.session()
            if parameters is not None:
                response = session.run(query, **parameters)
            else:
                response = session.run(query)
            response = [r.values() for r in response]
        except Exception as e:
            printm(e, 'Neo4j: Query Failed')
        finally: 
            if session is not None:
                session.close()
        return response


def get_mysql_connector():
    '''Connect to mysql as defined in docker compose file'''
    user, password = os.getenv('MYSQL_USER'), os.getenv('MYSQL_PASSWORD')
    host, database = os.getenv('MYSQL_HOST'), os.getenv('MYSQL_DATABASE')
    mysql_connector = MySQLConnector(user, password, host, database)
    return mysql_connector


def get_mongodb_connector():
    '''Connect to mongodb as defined in docker compose file'''
    mongodb_uri = os.getenv('ME_CONFIG_MONGODB_URL')
    mongodb_connector = MongoDBConnector(mongodb_uri)
    return mongodb_connector


def get_neo4j_connector():
    '''Connect to neo4j as defined in docker compose file'''
    uri = os.getenv('NEO4J_URL')
    username, password = os.getenv('NEO4J_AUTH').split('/')
    neo4j_connector = Neo4jConnector(uri=uri, user=username, pwd=password)
    return neo4j_connector


def get_kafka_uri():
    '''Get Kafka connection uri as defined in docker compose, or local connection uri in case we ran consumers outside docker'''
    try:
        _, uri = os.getenv('KAFKA_ADVERTISED_LISTENERS').split(',')[-1].split('//')
    except:
        uri = 'localhost:9092'
    return uri