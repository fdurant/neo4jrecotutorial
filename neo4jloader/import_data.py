from neo4j.v1 import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, ClientError, AuthError, SecurityError
from time import sleep
import sys
import os

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

cypher = "LOAD CSV WITH HEADERS"
cypher += 'FROM "file://groups.csv"'        
cypher += "AS row"
cypher += "RETURN row"
cypher += "CREATE (:Group { id:row, name:row, urlname:row.urlname, rating:toInt(row.rating), created:toInt(row.created))"
result = session.run(cypher)
result = session.run("MATCH (g:Group) RETURN g.id, g.name, g.urlname LIMIT 5")

for record in result:
    print >> sys.stderr, record

#cypher = "MERGE (node:Label )"