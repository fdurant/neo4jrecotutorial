from neo4j.v1 import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, ClientError, AuthError, SecurityError
from time import sleep
import sys
import os

global session

def initialize():

    global session
    global driver

    secondsToSleep = 10

    for i in range(5,0,-1):
        try:
            driver = GraphDatabase.driver("bolt://neo4j:7687", auth=basic_auth(os.environ['NEO4J_AUTH'].split('/')[0], "12345"))
            session = driver.session()
        except (ServiceUnavailable, ClientError, AuthError, SecurityError) as e:
            if i == 1:
                raise
            sleep(secondsToSleep)
            print('retry %d after catching %s; sleeping for %d seconds' % (i,str(e), secondsToSleep))
        else:
            break

def empty_db():

    try:
        result = session.run('MATCH (n) RETURN (n) LIMIT 1')
        res = result.peek()
        # Something found, so delete all nodes
        result = session.run('MATCH (n) DETACH DELETE(n)')
        print >> sys.stderr, "Emptying Neo4J database"
    except:
        print >> sys.stderr, "Neo4J database is already empty, so no need to empty it again"

def import_groups():

    cypher = "LOAD CSV WITH HEADERS"
    cypher += ' FROM "file:///groups.csv"'
    cypher += " AS row"
    cypher += " CREATE (:Group { id:row.id, name:row.name, urlname:row.urlname, rating:toInt(row.rating), created:toInt(row.created) })"
    result = session.run(cypher)
    for record in result:
        print >> sys.stderr, record

def show_groups():

    result = session.run("MATCH (g:Group) RETURN g.id, g.name, g.urlname LIMIT 5")

    for record in result:
        print >> sys.stderr, record

def merge_and_constraints():
    
    cypher = 'CREATE CONSTRAINT ON (t:Topic) ASSERT t.id IS UNIQUE;'
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

    cypher = ' CREATE CONSTRAINT ON (g:Group) ASSERT g.id IS UNIQUE;'
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def template():
    
    cypher = ''
    cypher += ' '
    cypher += ' '
    cypher += ' '
    cypher += ' '
    cypher += ' '
    cypher += ' '

    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record


if __name__ == "__main__":
    initialize()
    empty_db()
    import_groups()
    show_groups()
    merge_and_constraints()
