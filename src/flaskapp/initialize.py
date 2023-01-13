import argparse
from faker import Faker
import json
import os
import random
from kafka.admin import KafkaAdminClient, NewTopic

from flaskapp import node_from_name, print_friendships
from flaskapp.db import get_mysql_connector, get_mongodb_connector, get_neo4j_connector, get_kafka_uri


def load_products():
    '''Load products from json file'''
    with open('../products.json') as f:
        products = json.load(f)['products']
    return products


def populate_products_db(mongodb_connector, categories_num, keep_attrs=('title', 'description')):
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

    db = mongodb_connector.cnx['products']

    # Drop existing collections if any
    for collection in db.list_collection_names():
        print(f"MongoDB: Dropping collection: `{collection}`")
        db[collection].drop()

    # Populate Mongodb category collections
    product_obj_ids = []
    for category, products_per_category in products_by_category.items():
        print(f"MongoDB: Creating collection: `{category}`")
        inserted_ids = db[category].insert_many(products_per_category).inserted_ids
        product_obj_ids.extend(inserted_ids)

    # Close connection
    mongodb_connector.close()
    product_ids, categories = [str(obj_id) for obj_id in product_obj_ids], list(products_by_category.keys())
    return product_ids, categories


def initialize_mysql(mysql_connector):
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

    TABLES = {'transactions':transactions, 'products':products, 'categories':categories, 'users':users}
    
    # Drop tables if they exist
    for table_name in TABLES:
        mysql_connector.drop_table(table_name)

    # Create tables if not exist in backward ordr to handle foreign key dependencies
    for table_name, table_query in list(TABLES.items())[::-1]:
        mysql_connector.create_table(table_name, table_query)
    
    # Close connection
    mysql_connector.close()


def populate_users_db(neo4j_connector, product_ids, users_num=10):
    '''Create neo4j user nodes with name and product properties and friendship relationships'''
     # Delete all nodes and relationships
    neo4j_connector.query("MATCH (u) DETACH DELETE u")

    # Create fake users with 'name' and 'products' properties
    fake = Faker()
    product_upper_bound = len(product_ids) // users_num
    names_with_products = [(
            fake.name(),
            random.sample(product_ids, random.randint(1, product_upper_bound))
        ) for i in range(users_num)]

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
    # print_friendships(friendships)

    # Create and execute query
    query = f"CREATE {', '.join(query)}"
    neo4j_connector.query(query)
    # Close connection
    neo4j_connector.close()


def initialize_kafka(categories) -> None:
    '''Create topics proramatically during initialization phase. Not used'''
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
    # Initilize MongoDB database
    mongodb_connector = get_mongodb_connector()
    product_ids, categories = populate_products_db(mongodb_connector, args.categories_num)

    # Initilize Neo4j database
    neo4j_connector = get_neo4j_connector()
    populate_users_db(neo4j_connector, product_ids)

    # Initilize MySQL database
    mysql_connector = get_mysql_connector()
    initialize_mysql(mysql_connector)

    ## Initilize Kafka
    # initialize_kafka(categories)

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

    