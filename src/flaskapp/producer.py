import json
from kafka import KafkaProducer

from flaskapp import printm, doc_oid_to_str, user_node_to_dict
from flaskapp.initialize import get_mongo_client, get_neo4j_client, get_kafka_uri


def on_send_success(record_metadata):
    msg = f"Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}"
    printm(msg, 'Kafka Success')


def on_send_error(excp):
    printm(excp, 'Kafka Error')


def mongo_producer(collection_string):
    # Fetch collection data
    mongo_client = get_mongo_client()
    db = mongo_client['products']
    collections = db.list_collection_names()
    if collection_string not in collections:
        mongo_client.close()
        return {'response': 'Collection not found'}    
    collection = db[collection_string]
    products = [doc_oid_to_str(doc) for doc in collection.find({})]
    products = {product['_id']:{k:v for k,v in product.items() if k != '_id'} for product in products}
    mongo_client.close()
    
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
    # Connect to neo4j
    neo4j_client = get_neo4j_client()
    friends = neo4j_client.query(f"MATCH (u:User)--(f) WHERE ID(u) = {user_id} RETURN f")
    friends = [x for y in friends for x in y]
    user = neo4j_client.query(f"MATCH (u:User) WHERE ID(u) = {user_id} RETURN u")
    user = [x for y in user for x in y]
    users = [user_node_to_dict(user_node) for user_node in user + friends]
    neo4j_client.close()
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