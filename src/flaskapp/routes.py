from flask import Flask
from flaskapp.producer import mongo_producer, neo4j_producer
from flaskapp.consumer import mongo_consumer, neo4j_consumer, update_mysql_tables


app = Flask(__name__)


@app.route('/collection/<collection>', methods=['GET'])
def products_producer(collection):
    collection_data = mongo_producer(collection)
    return collection_data


@app.route('/user/<int:user_id>', methods=['GET'])
def users_producer(user_id):
    users_data = neo4j_producer(user_id)
    return users_data


@app.route('/collection/<collection>/user/<int:user_id>', methods=['GET'])
def users_products_consumer(collection, user_id):
    all_collection_products = mongo_consumer(collection)
    if all_collection_products is None:
        return {'response': 'Collection not found'}
    user_data = neo4j_consumer(user_id)
    if user_data is None:
        return {'response': 'User not found'}
    user_products_data = update_mysql_tables(collection, all_collection_products, user_data)
    return user_products_data