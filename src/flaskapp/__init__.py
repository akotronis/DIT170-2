from neo4j import graph
import string
from typing import Any


def printm(message: Any=None, title: str='', width: int=80) -> str:
    '''Print message prettier adding lines and title to make it clear within other outputs'''
    output = []
    title = title if not title else f" {title} ".center(width, '=') + '\n'
    output.append(f"{width*'='}\n{title}{width*'='}")
    if message is not None:
        output.append(str(message))
        output.append(f"{width*'='}\n{width*'='}")
    output = '\n'.join(output)
    print(output)
    return output


def node_from_name(name: str) -> str:
    '''Remove spaces and punctuation from users names to use as node labels'''
    node = ''.join([char for char in ''.join(name.split()) if char not in string.punctuation])
    return node


def doc_oid_to_str(doc: dict) -> dict:
    '''Convert object id to string'''
    doc['_id'] = str(doc['_id'])
    return doc


def print_friendships(friendships: dict) -> None:
    '''For each user, print user and friends, were friend is any node that is connected with user (not directed relationship)'''
    for user_node in friendships:
        print(f"User: {user_node}")
        friends = friendships[user_node] + [k for k,v in friendships.items() if user_node in v]
        for i, friend in enumerate(friends, 1):
            print(f"  {str(i).zfill(2)}: {friend}")


def user_node_to_dict(user_node: graph.Node) -> dict:
    '''Extract data from neo4j quet results'''
    user_dict = {
        'id': int(user_node.element_id.split(':')[-1]),
        'name': user_node['name'],
        'products': user_node['products'],
    }
    return user_dict


def kafka_cluster_metadata(client):
    '''Get kafka client metadata'''
    future = client.cluster.request_update()
    client.poll(future=future)
    metadata = client.cluster
    return metadata














