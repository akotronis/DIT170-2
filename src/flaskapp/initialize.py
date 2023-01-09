import argparse
from faker import Faker
import json
import os
import random
from kafka.admin import KafkaAdminClient, NewTopic
import mysql.connector
from pymongo import MongoClient

from flaskapp.neo import Neo4jConnection
from flaskapp import printm, node_from_name, print_friendships


def get_mysql_client():
    '''Return the mysql client according to docker-compose definition of the database
    '''
    user, password = os.getenv('MYSQL_USER'), os.getenv('MYSQL_PASSWORD')
    host, database = os.getenv('MYSQL_HOST'), os.getenv('MYSQL_DATABASE')
    try:
        client = mysql.connector.connect(user=user, password=password, database=database, host=host)
    except Exception as e:
        printm(e, "Failed to connect with MariaDB database:")
    return client


def initialize_mysql(client):
    '''Create MySQL database tables if they don't exist'''
    # Define queries for table creation
    users = '''
        CREATE TABLE IF NOT EXISTS users (
            id INT NOT NULL,
            name VARCHAR(255),
            PRIMARY KEY (id)
        );
    '''
    categories = '''
        CREATE TABLE IF NOT EXISTS categories (
            id INT NOT NULL AUTO_INCREMENT,
            title VARCHAR(255),
            PRIMARY KEY (id),
            UNIQUE KEY(title)
        );
    '''
    products = '''
        CREATE TABLE IF NOT EXISTS products (
            id VARCHAR(255) NOT NULL,
            title VARCHAR(255),
            description VARCHAR(255),
            category_id INT NOT NULL,
            PRIMARY KEY (id),
            FOREIGN KEY (category_id) REFERENCES categories(id)
        );
    '''
    transactions = '''
        CREATE TABLE IF NOT EXISTS transactions (
            id INT NOT NULL AUTO_INCREMENT,
            user_id INT NOT NULL,
            product_id VARCHAR(255) NOT NULL,
            timestamp DATETIME NOT NULL,
            PRIMARY KEY (id),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (product_id) REFERENCES products(id),
            UNIQUE KEY(user_id, product_id)
        );
    '''

    # Create tables if not exist
    TABLES = {'users':users, 'categories':categories, 'products':products, 'transactions':transactions}
    cursor = client.cursor()
    for table in TABLES:
        query = TABLES[table]
        try:
            printm(title=f"Creating table: `{table}`")
            cursor.execute(query)
        except mysql.connector.Error as e:
            printm(e, 'Mysql Error')
        else:
            printm(title=f'Mysql: Table `{table}` in place')
            cursor.execute(f"DELETE FROM {table}")
    
    # Close cursor and connection
    cursor.close()
    client.close()


def get_neo4j_client():
    '''Return the neo4j client according to docker-compose definition of the database
    '''
    uri = os.getenv('NEO4J_URL')
    username, password = os.getenv('NEO4J_AUTH').split('/')
    client = Neo4jConnection(uri=uri, user=username, pwd=password)
    return client 


def get_mongo_client():
    '''Return the mongodb client according to docker-compose definition of the database
    '''
    uri = os.getenv('ME_CONFIG_MONGODB_URL')
    try:
        client = MongoClient(host=uri)
    except Exception as e:
        printm(e, "Failed to connect with MongoDB database:")
    return client


def get_mongo_collections():
    '''Get mongoDB products database collections'''
    client = get_mongo_client()
    db = client['products']
    collections = db.list_collection_names()
    client.close()
    return collections


def load_products():
    '''Load products from json file'''
    with open('../products.json') as f:
        products = json.load(f)['products']
    return products


def get_kafka_uri():
    '''Get Kafka connection uri as defined in docker compose, or local connection uri in case we ran consumers outside docker'''
    try:
        _, uri = os.getenv('KAFKA_ADVERTISED_LISTENERS').split(',')[-1].split('//')
    except:
        uri = 'localhost:9092'
    return uri


def populate_products_db(client, categories_num, keep_attrs=('title', 'description')):
    '''Create mongoDB products database and category collections'''
    ########################## Products dummy data API #########################
    # https://dummyjson.com/products/categories                                #
    # https://dummyjson.com/products/category/{category}?limit={per_category}  #
    ############################################################################
    # Load products from json file
    products = load_products()

    # Create products by category dict
    products_by_category = {}
    for product in products:
        product_category = product['category']
        updated_product = {k:product[k] for k in keep_attrs}
        try:
            products_by_category[product_category].append(updated_product)
        except:
            products_by_category[product_category] = [updated_product]

    # filter `products_by_category`
    categories_num = min(categories_num, len(products_by_category))
    selected_categories = random.sample(list(products_by_category.keys()), categories_num)
    products_by_category = {k:v for k,v in products_by_category.items() if k in selected_categories}

    # Populate Mongodb category collections
    db = client['products']
    product_obj_ids = []
    for category, products_per_category in products_by_category.items():
        db[category].delete_many({})
        inserted_ids = db[category].insert_many(products_per_category).inserted_ids
        product_obj_ids.extend(inserted_ids)

    # Close connection
    client.close()
    product_ids, categories = [str(obj_id) for obj_id in product_obj_ids], list(products_by_category.keys())
    return product_ids, categories


def populate_users_db(client, product_ids, users_num=10):
    '''Create neo4j user nodes with name and product properties and friendship relationships'''
    # Create fake users with 'name' and 'products' properties
    fake = Faker()
    product_upper_bound = len(product_ids) // users_num
    names_with_products = [
        (
            fake.name(),
            random.sample(product_ids, random.randint(1, product_upper_bound))
        ) for i in range(users_num)
    ]

    # Delete all nodes and relationships
    client.query("MATCH (u) DETACH DELETE u")

    query = []
    # User nodes
    for name, products in names_with_products:
        node = node_from_name(name)
        query.append(f"({node}:User {{name:'{name}', products:{products}}})")
    
    # Friendship relationships
    friendships = {}
    user_nodes = [node_from_name(item[0]) for item in names_with_products]
    for user_node in user_nodes:
        allready_connected_to = [k for k,v in friendships.items() if user_node in v]
        other_nodes = [node for node in user_nodes if node not in [user_node] + allready_connected_to]
        friend_nodes = random.sample(other_nodes, random.randint(1, len(other_nodes)))
        query.extend([f"({user_node})-[:FRIEND]->({friend_node})" for friend_node in friend_nodes])
        friendships[user_node] = friend_nodes
    print_friendships(friendships)

    # Create and execute query
    query = f"CREATE {', '.join(query)}"
    client.query(query)
    # Close connection
    client.close()


def initialize_kafka(categories):
    uri = get_kafka_uri()

    # Create admin client to create and delete topics
    admin_client = KafkaAdminClient(bootstrap_servers=uri)
    current_topics = admin_client.list_topics()

    # # Delete existing topics (Better do it manually. It doesn't wait until they are deletd and exceeption is raised)
    # if current_topics:
    #     admin_client.delete_topics(topics=current_topics)
    #     while True:
    #         topics = admin_client.list_topics()
    #         print(topics)
    #         if not topics:
    #             break
    
    # Create topics that don't exist
    topic_names = [f"{category}" for category in categories] + ['users']
    topics_to_create = [
        NewTopic(name=topic, num_partitions=1, replication_factor=1)\
            for topic in topic_names\
            if topic not in current_topics
    ]
    admin_client.create_topics(new_topics=topics_to_create)


def main(args):
    mongo_client = get_mongo_client()
    product_ids, categories = populate_products_db(mongo_client, args.categories_num)
    neo4j_client = get_neo4j_client()
    populate_users_db(neo4j_client, product_ids)
    # initialize_kafka(categories)
    mariadb_client = get_mysql_client()
    initialize_mysql(mariadb_client)

#########################################################
#########################################################
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DIT170 - Project')
    parser.add_argument(
        '--categories-num',
        type=int,
        dest='categories_num',
        help='Number of product categories to use',
        default=8
    )
    args = parser.parse_args()
    main(args)

    