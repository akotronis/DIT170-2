from neo4j import GraphDatabase
from flaskapp import printm

class Neo4jConnection:
    
    def __init__(self, uri, user, pwd):
        self.uri = uri
        self.user = user
        self.pwd = pwd
        self.driver = None
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.pwd))
            printm('Neo4j driver created!')
        except Exception as e:
            printm(e, 'Failed to create the Neo4j driver...')
        
    def close(self):
        if self.driver is not None:
            self.driver.close()
        
    def query(self, query, parameters=None, db=None):
        assert self.driver is not None, printm(None, "Driver not initialized!")
        session, response = None, None
        try: 
            session = self.driver.session(database=db) if db is not None else self.driver.session()
            if parameters is not None:
                response = session.run(query, **parameters)
            else:
                response = session.run(query)
            response = [r.values() for r in response]
        except Exception as e:
            printm(e, 'Query Failed...')
        finally: 
            if session is not None:
                session.close()
        return response