import string
import mysql


def printm(message=None, title='', width=80):
    output = []
    title = title if not title else f" {title} ".center(width, '=') + '\n'
    output.append(f"{width*'='}\n{title}{width*'='}")
    if message is not None:
        output.append(str(message))
        output.append(f"{width*'='}\n{width*'='}")
    output = '\n'.join(output)
    print(output)
    return output


def node_from_name(name):
    '''Remove spaces and punctuation from users names to use as node labels'''
    node = ''.join([char for char in ''.join(name.split()) if char not in string.punctuation])
    return node


def kafka_cluster_metadata(client):
    future = client.cluster.request_update()
    client.poll(future=future)
    metadata = client.cluster
    return metadata


def doc_oid_to_str(doc):
    '''Convert object id to string'''
    doc['_id'] = str(doc['_id'])
    return doc


def print_friendships(friendships):
    '''For each user, print user and friends, were friend is any node that is connected with user (not directed relationship)'''
    for user_node in friendships:
        print(f"User: {user_node}")
        friends = friendships[user_node] + [k for k,v in friendships.items() if user_node in v]
        for i, friend in enumerate(friends, 1):
            print(f"  {str(i).zfill(2)}: {friend}")


def user_node_to_dict(user_node):
    '''Extract data from neo4j quet results'''
    user_dict = {
        'id': int(user_node.element_id.split(':')[-1]),
        'name': user_node['name'],
        'products': user_node['products'],
    }
    return user_dict


def insert_user_data(cnx, user_data):
    '''Insert user data to MySQL database table'''
    cursor = cnx.cursor()
    query = '''INSERT INTO users (id, name) VALUES (%(id)s, %(name)s)'''
    try:
        cursor.execute(query, user_data)
        cnx.commit()
    except mysql.connector.IntegrityError as err:
        cnx.rollback()
        printm(title=f'Users | Error: {err}', width=100)
    finally:
        cursor.close()


def insert_category_data(cnx, category):
    '''Insert category data to MySQL database table'''
    cursor = cnx.cursor()
    query1 = '''INSERT INTO categories (title) VALUES (%s)'''
    query2 = '''SELECT id FROM categories WHERE title=%s'''
    try:
        cursor.execute(query1, (category, ))
        cnx.commit()
    except mysql.connector.IntegrityError as err:
        cnx.rollback()
        printm(title=f'Categories | Error: {err}', width=100)
    finally:
        cursor.execute(query2, (category, ))
        category_id = cursor.fetchone()[0]
        cursor.close()
    return category_id
        

def insert_product_data(cnx, product_data):
    '''Insert product data to MySQL database table'''
    cursor = cnx.cursor()
    query = '''INSERT INTO products (id, title, description, category_id)
        VALUES (%(id)s, %(title)s, %(description)s, %(category_id)s)'''
    try:
        cursor.execute(query, product_data)
        cnx.commit()
    except mysql.connector.IntegrityError as err:
        cnx.rollback()
        printm(title=f'Products | Error: {err}', width=100)
    finally:
        cursor.close()


def insert_transaction_data(cnx, transaction_data):
    '''Insert transaction data to MySQL database table'''
    cursor = cnx.cursor()
    query = '''INSERT INTO transactions (user_id, product_id, timestamp)
        VALUES (%(user_id)s, %(product_id)s, %(timestamp)s)'''
    try:
        cursor.execute(query, transaction_data)
        cnx.commit()
    except mysql.connector.IntegrityError as err:
        cnx.rollback()
        printm(title=f'Transactions | Error: {err}', width=100)
    finally:
        cursor.close()