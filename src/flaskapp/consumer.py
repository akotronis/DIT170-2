from datetime import datetime
import json
from kafka import KafkaConsumer, TopicPartition

from flaskapp import printm
from flaskapp.db import get_mysql_connector, get_kafka_uri


def mongo_consumer(collection):
    '''Consume messages from kafka product collection topic as defined by API endpoint. Data are read from latest offset'''
    # Create Kafka Consumer for MongoDB
    uri = get_kafka_uri()
    prd_consumer = KafkaConsumer(
        bootstrap_servers=uri,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=100,
        # Can't `make auto_offset_reset='latest'` work. A workaround below
    )

    # If topic is not present, i.e. corresponding message has not been produced, return None
    if collection not in prd_consumer.topics():
        prd_consumer.close()
        return None

    # Read collection products from corresponding topic latest offset
    topic_partition = TopicPartition(topic=collection, partition=0)
    prd_consumer.assign([topic_partition])
    latest_topic_partition_offset = max(prd_consumer.end_offsets([topic_partition])[topic_partition] - 1, 0)
    prd_consumer.seek(topic_partition, latest_topic_partition_offset)
    all_collection_products = next(iter([message.value for message in prd_consumer]), {})
    prd_consumer.close()
    return all_collection_products
    

def neo4j_consumer(user_id):
    '''Consume messages from kafka users topic as defined by API endpoint. Data are read from latest offset that the user is found.'''
    # Create Kafka Consumer for Neo4j
    uri = get_kafka_uri()
    usr_consumer = KafkaConsumer(
        'users',
        bootstrap_servers=uri,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=100,
    )

    # Read user data from the latest offset that the `user_id` is found
    topic_partition = TopicPartition(topic='users', partition=0)
    end_offset = usr_consumer.end_offsets([topic_partition])[topic_partition]
    user_data = None
    for offset in list(range(end_offset))[::-1]:
        usr_consumer.seek(topic_partition, offset)
        message = next(usr_consumer)
        user_found_in_message = next(iter([user for user in message.value if user['id'] == user_id]), None)
        if user_found_in_message is not None:
            ts = datetime.utcfromtimestamp(message.timestamp // 1000)
            user_data = {**user_found_in_message, 'timestamp':ts.strftime('%Y-%m-%d %H:%M:%S')}
            break
    usr_consumer.close()
    return user_data


def update_mysql_tables(collection, all_collection_products, user_data):
    '''Populate MySQL databases with consumed users and products data'''
    # Find user products that are in the given `collection` and update MySQL database tables
    # Connect to MySQL database
    mysql_connector = get_mysql_connector()
    # Insert user data
    _user_data = {'id':user_data['id'], 'name':user_data['name']}
    mysql_connector.insert_user_data(_user_data)
    # Insert category data
    category_id = mysql_connector.insert_category_data(collection)
    products = []
    # Search for user products in given collection
    user_products_in_collection = set(user_data['products']).intersection(all_collection_products.keys())
    for product_id in user_products_in_collection:
        product_data = {'id':product_id, 'category_id':category_id, **all_collection_products[product_id]}
        mysql_connector.insert_product_data(product_data)
        transaction_data = {'user_id':user_data['id'], 'product_id':product_id, 'timestamp':user_data['timestamp']}
        mysql_connector.insert_transaction_data(transaction_data)
        products.append(product_data)
    mysql_connector.close()
    user_data['products'] = products
    return user_data
    


if __name__ == '__main__':
    neo4j_consumer(10)
    # mongo_consumer('fragrances')