import json
from kafka import KafkaProducer

from flaskapp import printm, doc_oid_to_str, user_node_to_dict
from flaskapp.db import get_mongodb_connector, get_neo4j_connector, get_kafka_uri


def on_send_success(record_metadata):
    '''Print message when succefully send to kafka'''
    msg = f"Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}"
    printm(msg, 'Kafka Success')


def on_send_error(excp):
    '''Print message when fail to send to kafka'''
    printm(excp, 'Kafka Error')


def mongo_producer(collection_string):
    '''Produce kafka message with mongodb products collection contents as defined by API endpoint'''
    # Fetch collection data
    mongodb_connector = get_mongodb_connector()
    db = mongodb_connector.cnx['products']
    product_collections = db.list_collection_names()
    if collection_string not in product_collections:
        mongodb_connector.close()
        return {'response': 'Collection not found'}    
    collection = db[collection_string]
    products = [doc_oid_to_str(doc) for doc in collection.find({})]
    products = {product['_id']:{k:v for k,v in product.items() if k != '_id'} for product in products}
    mongodb_connector.close()
    
    # Create Kafka producer
    uri = get_kafka_uri()
    producer = KafkaProducer(
        bootstrap_servers=uri,
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )

    # Send message to Kafka
    producer.send(
        f"{collection_string}",
        products,
    ).add_callback(on_send_success).add_errback(on_send_error)
    producer.flush()
    return products


def neo4j_producer(user_id):
    '''Produce kafka message with neo4j user-friends as defined by API endpoint'''
    # Connect to neo4j
    neo4j_connector = get_neo4j_connector()
    friends = neo4j_connector.query(f"MATCH (u:User)--(f) WHERE ID(u) = {user_id} RETURN f")
    friends = [x for y in friends for x in y]
    user = neo4j_connector.query(f"MATCH (u:User) WHERE ID(u) = {user_id} RETURN u")
    neo4j_connector.close()
    user = [x for y in user for x in y]
    users = [user_node_to_dict(user_node) for user_node in user + friends]
    if not user:
        return {'response': 'User not found'} 
    
    # Create Kafka producer
    uri = get_kafka_uri()
    producer = KafkaProducer(
        bootstrap_servers=uri,
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )

    # Send message to Kafka
    if users:
        producer.send(
            "users",
            users,
        ).add_callback(on_send_success).add_errback(on_send_error)
        producer.flush()
    return users