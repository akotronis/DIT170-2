from datetime import datetime
import json
from kafka import KafkaConsumer, TopicPartition

from flaskapp import insert_user_data, insert_category_data, insert_product_data, insert_transaction_data
from flaskapp.initialize import get_mongo_collections, get_kafka_uri, get_mysql_client


def get_latest_offset(consumer, topic, partition):
    topic_partition = TopicPartition(topic=topic, partition=partition)
    latest_topic_partition_offset = max(consumer.end_offsets([topic_partition])[topic_partition] - 1, 0)
    consumer.seek(topic_partition, latest_topic_partition_offset)
    return consumer


def neo4j_mongo_consumer(collection, user_id):
    mongo_collections = get_mongo_collections()
    if collection not in mongo_collections:
        return {'response': 'Collection not found'}

    # Create Kafka Consumer for MongoDB
    uri = get_kafka_uri()
    prd_consumer = KafkaConsumer(
        collection,
        bootstrap_servers=uri,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=100,
        # Can't `make auto_offset_reset='latest'` work
    )
    # Read collection products from latest offset
    prd_consumer = get_latest_offset(prd_consumer, collection, 0)
    all_collection_products = next(iter([message.value for message in prd_consumer]), {})

    # Create Kafka Consumer for Neo4j
    usr_consumer = KafkaConsumer(
        'users',
        bootstrap_servers=uri,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=100,
        auto_offset_reset='earliest'
    )
    # Read user data from the latest offset that the `user_id` is found
    user_data = None
    for message in usr_consumer:
        user_found_in_message = next(iter([user for user in message.value if user['id'] == user_id]), None)
        if user_found_in_message is not None:
            ts = datetime.utcfromtimestamp(message.timestamp // 1000)
            user_data = {**user_found_in_message, 'timestamp':ts.strftime('%Y-%m-%d %H:%M:%S')}

    # Find user products that are in the given `collection` and update MySQL database tables
    if user_data:
        # Connect to MySQL database
        cnx = get_mysql_client()
        # Insert user data
        _user_data = {'id':user_data['id'], 'name':user_data['name']}
        insert_user_data(cnx, _user_data)
        # Insert category data
        category_id = insert_category_data(cnx, collection)
        products = []
        # Search for user products in given collection
        user_products_in_collection = set(user_data['products']).intersection(all_collection_products.keys())
        for product_id in user_products_in_collection:
            product_data = {'id':product_id, 'category_id':category_id, **all_collection_products[product_id]}
            insert_product_data(cnx, product_data)
            transaction_data = {'user_id':user_data['id'], 'product_id':product_id, 'timestamp':user_data['timestamp']}
            insert_transaction_data(cnx, transaction_data)
            products.append(product_data)
        cnx.close()
        user_data['products'] = products
        return user_data
    return {'response': 'User not found'}


if __name__ == '__main__':
    # neo4j_mongo_consumer('automotive', 1)
    # neo4j_mongo_consumer('fragrances', 1)
    # neo4j_mongo_consumer('furniture', 1)
    # neo4j_mongo_consumer('groceries', 1)
    # neo4j_mongo_consumer('home-decoration', 6)
    # neo4j_mongo_consumer('sunglasses', 6)
    # neo4j_mongo_consumer('laptops', 1)
    # neo4j_mongo_consumer('lighting', 1)
    # neo4j_mongo_consumer('mens-shirts', 1)
    # neo4j_mongo_consumer('mens-shoes', 1)
    # neo4j_mongo_consumer('mens-watches', 1)
    neo4j_mongo_consumer('motorcycle', 21)
    # neo4j_mongo_consumer('smartphones', 1)
    # neo4j_mongo_consumer('tops', 2)