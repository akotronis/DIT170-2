from flask import Flask
from flaskapp.producers import mongo_producer, neo4j_producer
from flaskapp.consumers import neo4j_mongo_consumer


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
    users_products_data = neo4j_mongo_consumer(collection, user_id)
    return users_products_data