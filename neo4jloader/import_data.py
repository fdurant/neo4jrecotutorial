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
    print >> sys.stderr, "CYPHER = ", cypher
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

def show_constraints():
    
    cypher = 'CALL db.constraints()'
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record    

def import_topics():

    cypher = "LOAD CSV WITH HEADERS"
    cypher += ' FROM "file:///groups_topics.csv"'
    cypher += ' AS row'
    cypher += ' MERGE (topic:Topic {id: row.id})'
    cypher += ' ON CREATE SET topic.name = row.name, topic.urlkey = row.urlkey'

    result = session.run(cypher)
    for record in result:
        print >> sys.stderr, record

def show_topics():

    result = session.run("MATCH (t:Topic) RETURN t.id, t.name LIMIT 10")

    for record in result:
        print >> sys.stderr, record

def connect_groups_and_topics():
    
    cypher = 'LOAD CSV WITH HEADERS FROM "file:///groups_topics.csv"  AS row'
    cypher += ' MATCH (topic:Topic {id: row.id})'
    cypher += ' MATCH (group:Group {id: row.groupId})'
    cypher += ' MERGE (group)-[:HAS_TOPIC]->(topic)'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def show_groups_and_topics():

    result = session.run("MATCH (group:Group)-[:HAS_TOPIC]->(topic:Topic) RETURN group, topic LIMIT 10")

    for record in result:
        print >> sys.stderr, record

def create_indexes_on_groups_and_topics():
    
    cypher = 'CREATE INDEX ON :Group(name)'
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

    cypher = 'CREATE INDEX ON :Topic(name)'
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def show_indices_on_groups_and_topics():
    
    cypher = 'CALL db.indexes()'
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record    

def show_most_popular_topics():
    
    cypher = 'MATCH (t:Topic)<-[:HAS_TOPIC]-()'
    cypher += ' RETURN t.name, COUNT(*) AS count'
    cypher += ' ORDER BY count DESC'
    cypher += ' LIMIT 10'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def show_most_recently_created_group():
    
    cypher = 'MATCH (g:Group)'
    cypher += ' RETURN g'
    cypher += ' ORDER BY g.created DESC'
    cypher += ' LIMIT 1'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def show_groups_running_for_more_than_4_years():

    cypher = 'MATCH (g:Group)'
    cypher += ' WHERE (timestamp() - g.created) / 1000 / 3600 / 24 / 365 >= 4'
    cypher += ' RETURN count(g)'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def find_groups_with_neo4j_or_data_in_their_name():
    
    cypher = 'MATCH (g:Group)'
    cypher += " WHERE g.name CONTAINS 'Neo4j' OR g.name CONTAINS 'Data'"
    cypher += ' RETURN g'
    cypher += ' LIMIT 10'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def show_distinct_topics_for_these_groups():
    
    cypher = 'MATCH (g:Group)-[:HAS_TOPIC]->(t:Topic)'
    cypher += " WHERE g.name CONTAINS 'Neo4j' OR g.name CONTAINS 'Data'"
    cypher += ' RETURN t.name, count(*)'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def find_similar_groups_to_neo4j():

    cypher = 'MATCH (group:Group)'
    cypher += " WHERE (group.name CONTAINS 'Graph Database' OR group.name CONTAINS 'Neo4j')"
    cypher += ' MATCH (group)-[:HAS_TOPIC]->(topic)<-[:HAS_TOPIC]-(otherGroup)'
    cypher += ' RETURN otherGroup.name, COUNT(topic) AS topicsInCommon, COLLECT(topic.name) AS topics'
    cypher += ' ORDER BY topicsInCommon DESC, otherGroup.name'
    cypher += ' LIMIT 10'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record    

def explore_members():

    cypher = 'LOAD CSV WITH HEADERS'
    cypher += ' FROM "file:///members.csv" AS row'
    cypher += ' RETURN row'
    cypher += ' LIMIT 10'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)
    for record in result:
        print >> sys.stderr, record

def add_constraint_on_members():

    cypher = 'CREATE CONSTRAINT ON (m:Member) ASSERT m.id IS UNIQUE'
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def import_members():

    cypher = 'USING PERIODIC COMMIT 10000'
    cypher += ' LOAD CSV WITH HEADERS'
    cypher += ' FROM "file:///members.csv" AS row'
    cypher += ' WITH DISTINCT row.id AS id, row.name AS name'
    cypher += ' MERGE (member:Member {id: id})'
    cypher += ' ON CREATE SET member.name = name'

    print >> sys.stderr, "CYPHER = ", cypher
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

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record


if __name__ == "__main__":
    initialize()

    # The Meetup dataset
    empty_db()
    import_groups()
    show_groups()
    merge_and_constraints()
    show_constraints()
    import_topics()
    show_topics()
    connect_groups_and_topics()
    show_groups_and_topics()
    create_indexes_on_groups_and_topics()
    show_indices_on_groups_and_topics()
    
    # Exercises
    show_most_popular_topics()
    show_most_recently_created_group()
    show_groups_running_for_more_than_4_years()
    find_groups_with_neo4j_or_data_in_their_name()
    show_distinct_topics_for_these_groups()

    find_similar_groups_to_neo4j()
    
    # Members
    explore_members()
    add_constraint_on_members()
    import_members()
