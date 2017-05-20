from neo4j.v1 import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable, ClientError, AuthError, SecurityError
from time import sleep
import sys
import os

global session

dataImportOnly = True if os.environ['DATA_IMPORT_ONLY'] == '1' else False
print >> sys.stderr, "dataImportOnly = ", dataImportOnly

neo4j_uid, neo4j_pwd = os.environ['NEO4J_AUTH'].split('/')
#print >> sys.stderr, "neo4j_uid = ", neo4j_uid
#print >> sys.stderr, "neo4j_pwd = ", neo4j_pwd

def initialize():

    global session
    global driver

    secondsToSleep = 10

    for i in range(15,0,-1):
        try:
            driver = GraphDatabase.driver("bolt://neo4j:7687", auth=basic_auth(neo4j_uid, neo4j_pwd))
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
        print >> sys.stderr, "Started emptying Neo4J database"
        result = session.run('MATCH (n) DETACH DELETE(n)')
        print >> sys.stderr, "Done emptying Neo4J database"
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

def show_members():

    result = session.run("MATCH (member:Member)-[membership:MEMBER_OF]->(group) RETURN member, group, membership LIMIT 10")

    for record in result:
        print >> sys.stderr, record

def create_index_on_members():

    cypher = 'CREATE INDEX ON :Member(name)'
    print >> sys.stderr, "CYPHER = ", cypher

    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def find_some_member():

    cypher = 'MATCH (m:Member)'
    cypher += " WHERE m.name = 'Pieter Cailliau'"
    cypher += ' RETURN m'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record    

def find_member_of_how_many_groups():
    
    cypher = 'MATCH (m:Member)-[:MEMBER_OF]->(group)'
    cypher += " WHERE m.name = 'Pieter Cailliau'"
    cypher += ' RETURN group'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def find_topics_of_these_groups():

    cypher = 'MATCH (m:Member)-[:MEMBER_OF]->(group), (group)-[:HAS_TOPIC]->(topic)'
    cypher += " WHERE m.name = 'Pieter Cailliau'"
    cypher += ' RETURN group, topic'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record    

def find_topics_that_show_up_most():

    cypher = 'MATCH (m:Member)-[:MEMBER_OF]->(group), (group)-[:HAS_TOPIC]->(topic)'
    cypher += " WHERE m.name = 'Pieter Cailliau'"
    cypher += ' RETURN topic.name, COUNT(*) AS times'
    cypher += ' ORDER BY times DESC'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def exclude_groups_im_a_member_of():

    cypher = "MATCH (member:Member) WHERE member.name = 'Pieter Cailliau'"
    cypher += ' MATCH (group:Group)'
    cypher += " WHERE group.name CONTAINS 'Neo4j' OR group.name CONTAINS 'Neo4j'"
    cypher += ' MATCH (group)-[:HAS_TOPIC]->(topic)<-[:HAS_TOPIC]-(otherGroup:Group)'
    cypher += ' WHERE NOT EXISTS( (member)-[:MEMBER_OF]->(otherGroup) )'
    cypher += ' RETURN otherGroup.name,'
    cypher += '        COUNT(topic) AS topicsInCommon,'
    cypher += '        COLLECT(topic.name) AS topics'
    cypher += ' ORDER BY topicsInCommon DESC'
    cypher += ' LIMIT 10'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def find_my_similar_groups():

    cypher = "MATCH (member:Member) WHERE member.name CONTAINS 'Mark Needham'"
    cypher += ' MATCH (member)-[:MEMBER_OF]->()-[:HAS_TOPIC]->()<-[:HAS_TOPIC]-(otherGroup:Group)'
    cypher += ' WHERE NOT EXISTS ((member)-[:MEMBER_OF]->(otherGroup))'
    cypher += ' RETURN otherGroup.name,'
    cypher += '        COUNT(*) AS topicsInCommon'
    cypher += ' ORDER BY topicsInCommon DESC'
    cypher += ' LIMIT 10'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record    

def who_are_members_of_the_most_groups():

    cypher = 'MATCH (member:Member)-[:MEMBER_OF]->()'
    cypher += ' WITH member, COUNT(*) AS groups'
    cypher += ' ORDER BY groups DESC'
    cypher += ' LIMIT 10'
    cypher += ' RETURN member.name, groups'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def create_interested_in_relationship():

    cypher = 'USING PERIODIC COMMIT 10000'
    cypher += ' LOAD CSV WITH HEADERS FROM "file:///members.csv" AS row'
    cypher += ' WITH split(row.topics, ";") AS topics, row.id AS memberId'
    cypher += ' UNWIND topics AS topicId'
    cypher += ' WITH DISTINCT memberId, topicId'
    cypher += ' MATCH (member:Member {id: memberId})'
    cypher += ' MATCH (topic:Topic {id: topicId})'
    cypher += ' MERGE (member)-[:INTERESTED_IN]->(topic)'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def find_my_similar_groups():

    cypher = "MATCH (member:Member {name: 'Will Lyon'})-[:INTERESTED_IN]->(topic),"
    cypher += ' (member)-[:MEMBER_OF]->(group)-[:HAS_TOPIC]->(topic)'
    cypher += ' WITH member, topic, COUNT(*) AS score'
    cypher += ' MATCH (topic)<-[:HAS_TOPIC]-(otherGroup)'
    cypher += ' WHERE NOT (member)-[:MEMBER_OF]->(otherGroup)'
    cypher += ' RETURN otherGroup.name, COLLECT(topic.name), SUM(score) as score'
    cypher += ' ORDER BY score DESC'
    cypher += ' LIMIT 10'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def create_constraints_and_indexes_on_events():
    
    cypher = 'CREATE CONSTRAINT ON (e:Event) ASSERT e.id IS UNIQUE'
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

    cypher = 'CREATE INDEX ON :Event(time)'
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record    

def import_events():

    cypher = 'USING PERIODIC COMMIT 1000'
    cypher += ' LOAD CSV WITH HEADERS FROM "file:///events.csv" AS row'
    cypher += ' MERGE (event:Event {id: row.id})'
    cypher += ' ON CREATE SET event.name = row.name,'
    cypher += '               event.time = toInt(row.time),'
    cypher += '               event.utcOffset = toInt(row.utc_offset)'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def show_events():

    cypher = 'MATCH (event:Event)'
    cypher += ' RETURN event'
    cypher += ' LIMIT 10'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def connect_events_and_groups():

    cypher = 'USING PERIODIC COMMIT 1000'
    cypher += ' LOAD CSV WITH HEADERS FROM "file:///events.csv" AS row'
    cypher += ' WITH distinct row.group_id as groupId, row.id as eventId'
    cypher += ' MATCH (group:Group {id: groupId})'
    cypher += ' MATCH (event:Event {id: eventId})'
    cypher += ' MERGE (group)-[:HOSTED_EVENT]->(event)'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def show_groups_and_events():

    cypher = 'MATCH (group:Group)-[hosted:HOSTED_EVENT]->(event)'
    cypher += ' WHERE group.name STARTS WITH "NYC Neo4j" AND event.time < timestamp()'
    cypher += ' RETURN event, group, hosted'
    cypher += ' ORDER BY event.time DESC'
    cypher += ' LIMIT 10'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def find_future_events_in_my_groups():

    cypher = 'MATCH (member:Member)-[:MEMBER_OF]->(group)-[:HOSTED_EVENT]->(futureEvent)'
    cypher += " WHERE member.name CONTAINS 'Will Lyon' AND futureEvent.time > timestamp()"
    cypher += ' RETURN group.name,'
    cypher += '        futureEvent.name,'
    cypher += '        round((futureEvent.time - timestamp()) / (24.0*60*60*1000)) AS days'
    cypher += ' ORDER BY days'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def find_future_events_for_my_topics():
    
    cypher = "MATCH (member:Member) WHERE member.name CONTAINS 'Will Lyon'"
    cypher += ' MATCH (member)-[:INTERESTED_IN]->()<-[:HAS_TOPIC]-()-[:HOSTED_EVENT]->(futureEvent) WHERE futureEvent.time > timestamp()'
    cypher += ' WITH member, futureEvent, EXISTS((member)-[:MEMBER_OF]->()-[:HOSTED_EVENT]->(futureEvent)) AS myGroup'
    cypher += ' WITH member, futureEvent, myGroup, COUNT(*) AS commonTopics'
    cypher += ' MATCH (futureEvent)<-[:HOSTED_EVENT]-(group)'
    cypher += ' RETURN futureEvent.name, futureEvent.time, group.name, commonTopics, myGroup'
    cypher += ' ORDER BY futureEvent.time'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def filter_out_events_which_have_less_than_3_common_topics():
    
    cypher = "MATCH (member:Member) WHERE member.name CONTAINS 'Mark Needham'"
    cypher += ' MATCH (futureEvent:Event) WHERE futureEvent.time > timestamp()'
    cypher += ' WITH member, futureEvent, EXISTS((member)-[:MEMBER_OF]->()-[:HOSTED_EVENT]->(futureEvent)) AS myGroup'
    cypher += ' OPTIONAL MATCH (member)-[:INTERESTED_IN]->()<-[:HAS_TOPIC]-()-[:HOSTED_EVENT]->(futureEvent)'
    cypher += ' WITH member, futureEvent, myGroup, COUNT(*) AS commonTopics'
    cypher += ' WHERE commonTopics >= 3'
    cypher += ' MATCH (futureEvent)<-[:HOSTED_EVENT]-(group)'
    cypher += ' RETURN futureEvent.name, futureEvent.time, group.name, commonTopics, myGroup'
    cypher += ' ORDER BY futureEvent.time'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def only_show_events_happening_in_the_next_7_days():

    cypher = "MATCH (member:Member) WHERE member.name CONTAINS 'Mark Needham'"
    cypher += ' MATCH (futureEvent:Event)'
    # Here's the timestamp
    cypher += ' WHERE timestamp() + (7 * 24 * 60 * 60 * 1000) > futureEvent.time > timestamp()'
    cypher += ' WITH member, futureEvent, EXISTS((member)-[:MEMBER_OF]->()-[:HOSTED_EVENT]->(futureEvent)) AS myGroup'
    cypher += ' OPTIONAL MATCH (member)-[:INTERESTED_IN]->()<-[:HAS_TOPIC]-()-[:HOSTED_EVENT]->(futureEvent)'
    cypher += ' WITH member, futureEvent, myGroup, COUNT(*) AS commonTopics'
    cypher += ' WHERE commonTopics >= 3'
    cypher += ' MATCH (futureEvent)<-[:HOSTED_EVENT]-(group)'
    cypher += ' RETURN futureEvent.name, futureEvent.time, group.name, commonTopics, myGroup'
    # And here we order by it
    cypher += ' ORDER BY futureEvent.time'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def sorting_the_events_by_some_importance_score():

    cypher = "MATCH (member:Member) WHERE member.name CONTAINS 'Will Lyon'"
    cypher += ' MATCH (member)-[:INTERESTED_IN]->()<-[:HAS_TOPIC]-()-[:HOSTED_EVENT]->(futureEvent)'
    cypher += ' WHERE timestamp() + (7 * 24 * 60 * 60 * 1000) > futureEvent.time > timestamp()'
    cypher += ' WITH member, futureEvent, EXISTS((member)-[:MEMBER_OF]->()-[:HOSTED_EVENT]->(futureEvent)) AS myGroup'
    cypher += ' WITH member, futureEvent, myGroup, COUNT(*) AS commonTopics'
    cypher += ' WHERE commonTopics >= 3'
    cypher += ' MATCH (futureEvent)<-[:HOSTED_EVENT]-(group)'
    # define myGroupScore
    cypher += ' WITH futureEvent, group, commonTopics, myGroup, CASE WHEN myGroup THEN 5 ELSE 0 END AS myGroupScore'
    cypher += ' WITH futureEvent, group, commonTopics, myGroup, myGroupScore, round((futureEvent.time - timestamp()) / (24.0*60*60*1000)) AS days'
    # and integrate with other score elements
    cypher += ' RETURN futureEvent.name, futureEvent.time, group.name, commonTopics, myGroup, days, myGroupScore + commonTopics - days AS score'
    cypher += ' ORDER BY score DESC'
    cypher += ' LIMIT 10'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def create_constraint_on_venues():


    cypher = 'CREATE CONSTRAINT ON (v:Venue)'
    cypher += ' ASSERT v.id IS UNIQUE'

    result = session.run(cypher)
    for record in result:
        print >> sys.stderr, record

def import_venues():

    cypher = 'LOAD CSV WITH HEADERS FROM "file:///venues.csv" AS row'
    cypher += ' MERGE (venue:Venue {id: row.id})'
    cypher += ' ON CREATE SET venue.name = row.name,'
    cypher += ' venue.latitude = tofloat(row.lat),'
    cypher += ' venue.longitude = tofloat(row.lon),'
    cypher += ' venue.address = row.address_1'

    result = session.run(cypher)
    for record in result:
        print >> sys.stderr, record

def connect_events_to_venues():

    cypher = 'USING PERIODIC COMMIT 1000'
    cypher += ' LOAD CSV WITH HEADERS FROM "file:///events.csv" AS row'
    cypher += ' MATCH (venue:Venue {id: row.venue_id})'
    cypher += ' MATCH (event:Event {id: row.id})'
    cypher += ' MERGE (event)-[:VENUE]->(venue)'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def verify_venues_import():

    cypher = 'MATCH (venue:Venue)'
    cypher += ' WHERE EXISTS(venue.latitude) AND EXISTS(venue.longitude)'
    cypher += ' RETURN COUNT(*)'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def find_venues_near_here():
    
    # 'here' means the Skills Matter / CodeNode hacker space in London
    cypher = 'WITH point({latitude: 40.7577898, longitude: -73.9853772}) AS trainingVenue'
    cypher += ' MATCH (venue:Venue)'
    cypher += ' WITH venue, distance(point(venue), trainingVenue) AS distance'
    cypher += ' RETURN venue.id, venue.name, venue.address, distance'
    cypher += ' ORDER BY distance'
    cypher += ' LIMIT 10'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def update_recommender_to_also_return_distance_to_venue():
    
    cypher = "MATCH (member:Member) WHERE member.name CONTAINS 'Mark Needham'"
    cypher += ' MATCH (futureEvent:Event)'
    cypher += ' WHERE timestamp() + (7 * 24 * 60 * 60 * 1000) > futureEvent.time > timestamp()'
    cypher += ' WITH member, futureEvent, EXISTS((member)-[:MEMBER_OF]->()-[:HOSTED_EVENT]->(futureEvent)) AS myGroup'
    cypher += ' OPTIONAL MATCH (member)-[:INTERESTED_IN]->()<-[:HAS_TOPIC]-()-[:HOSTED_EVENT]->(futureEvent)'
    cypher += ' WITH member, futureEvent, myGroup, COUNT(*) AS commonTopics'
    cypher += ' WHERE commonTopics >= 3'
    cypher += ' MATCH (venue)<-[:VENUE]-(futureEvent)<-[:HOSTED_EVENT]-(group)'
    # Introduce distance
    cypher += ' WITH futureEvent, group, venue, commonTopics, myGroup, distance(point(venue), point({latitude: 51.518698, longitude: -0.086146})) AS distance'
    cypher += ' WITH futureEvent, group, venue, commonTopics, myGroup, distance, CASE WHEN myGroup THEN 5 ELSE 0 END AS myGroupScore'
    cypher += ' WITH futureEvent, group, venue, commonTopics, myGroup, distance, myGroupScore, round((futureEvent.time - timestamp()) / (24.0*60*60*1000)) AS days'
    # Include distance in returned values
    cypher += ' RETURN futureEvent.name, futureEvent.time, group.name, venue.name, commonTopics, myGroup, days, distance , myGroupScore + commonTopics - days AS score'
    cypher += ' ORDER BY score DESC'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def filter_out_events_further_than_1km():

    cypher = "MATCH (member:Member) WHERE member.name CONTAINS 'Mark Needham'"
    cypher += ' MATCH (futureEvent:Event)'
    cypher += ' WHERE timestamp() + (7 * 24 * 60 * 60 * 1000) > futureEvent.time > timestamp()'
    cypher += ' WITH member, futureEvent, EXISTS((member)-[:MEMBER_OF]->()-[:HOSTED_EVENT]->(futureEvent)) AS myGroup'
    cypher += ' OPTIONAL MATCH (member)-[:INTERESTED_IN]->()<-[:HAS_TOPIC]-()-[:HOSTED_EVENT]->(futureEvent)'
    cypher += ' WITH member, futureEvent, myGroup, COUNT(*) AS commonTopics'
    cypher += ' WHERE commonTopics >= 3'
    cypher += ' MATCH (venue)<-[:VENUE]-(futureEvent)<-[:HOSTED_EVENT]-(group)'
    cypher += ' WITH  futureEvent, group, venue,commonTopics, myGroup, distance(point(venue), point({latitude: 51.518698, longitude: -0.086146})) AS distance'
    # Limit distance
    cypher += ' WHERE distance < 1000'
    cypher += ' WITH futureEvent, group, venue, commonTopics, myGroup, distance, CASE WHEN myGroup THEN 5 ELSE 0 END AS myGroupScore'
    cypher += ' WITH futureEvent, group, venue, commonTopics, myGroup, distance, myGroupScore, round((futureEvent.time - timestamp()) / (24.0*60*60*1000)) AS days'
    cypher += ' RETURN futureEvent.name, futureEvent.time, group.name, venue.name, commonTopics, myGroup, days, distance, myGroupScore + commonTopics - days AS score'
    cypher += ' ORDER BY score DESC'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def import_rsvps():
    
    cypher = 'USING PERIODIC COMMIT 10000'
    cypher += ' LOAD CSV WITH HEADERS FROM "file:///rsvps.csv" AS row'
    cypher += ' WITH row WHERE row.response = "yes"'
    cypher += ' MATCH (member:Member {id: row.member_id})'
    cypher += ' MATCH (event:Event {id: row.event_id})'
    cypher += ' MERGE (member)-[rsvp:RSVPD {id: row.rsvp_id}]->(event)'
    cypher += ' ON CREATE SET rsvp.created = toint(row.created),'
    cypher += '               rsvp.lastModified = toint(row.mtime),'
    cypher += '               rsvp.guests = toint(row.guests)'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def include_attendance_at_previous_events_to_score():

    cypher = "MATCH (member:Member) WHERE member.name CONTAINS 'Will Lyon'"
    cypher += ' MATCH (futureEvent:Event)'
    cypher += ' WHERE timestamp() + (7 * 24 * 60 * 60 * 1000) > futureEvent.time > timestamp()'
    cypher += ' WITH member, futureEvent, EXISTS((member)-[:MEMBER_OF]->()-[:HOSTED_EVENT]->(futureEvent)) AS myGroup'
    cypher += ' OPTIONAL MATCH (member)-[:INTERESTED_IN]->()<-[:HAS_TOPIC]-()-[:HOSTED_EVENT]->(futureEvent)'
    cypher += ' WITH member, futureEvent, myGroup, COUNT(*) AS commonTopics'
    cypher += ' WHERE commonTopics >= 3'
    # Match previous events, if any
    cypher += ' OPTIONAL MATCH (member)-[rsvp:RSVPD]->(previousEvent)<-[:HOSTED_EVENT]-()-[:HOSTED_EVENT]->(futureEvent)'
    cypher += ' WHERE previousEvent.time < timestamp()'
    # Count attendance (well, RSVPs as a proxy of attendance)
    cypher += ' WITH futureEvent, commonTopics, myGroup, COUNT(rsvp) AS previousEvents'
    cypher += ' MATCH (venue)<-[:VENUE]-(futureEvent)<-[:HOSTED_EVENT]-(group)'
    cypher += ' WITH futureEvent, group, venue, commonTopics, myGroup, previousEvents, distance(point(venue), point({latitude: 40.7577898, longitude: -73.9853772})) AS distance'
    cypher += ' WITH futureEvent, group, venue, commonTopics, myGroup, previousEvents, distance, CASE WHEN myGroup THEN 5 ELSE 0 END AS myGroupScore'
    cypher += ' WITH futureEvent, group, venue, commonTopics, myGroup, previousEvents, distance, myGroupScore, round((futureEvent.time - timestamp()) / (24.0*60*60*1000)) AS days'
    # Include this count as a factor in the overall score
    cypher += ' RETURN futureEvent.name, futureEvent.time, group.name, venue.name, commonTopics, myGroup, previousEvents, days, distance, myGroupScore + commonTopics - days AS score'
    cypher += ' ORDER BY score DESC'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def calculate_score_for_events_at_previously_visited_venues():

    cypher = "MATCH (member:Member) WHERE member.name CONTAINS 'Mark Needham'"
    cypher += ' MATCH (futureEvent:Event)'
    cypher += ' WHERE timestamp() + (7 * 24 * 60 * 60 * 1000) > futureEvent.time > timestamp()'
    cypher += ' WITH member, futureEvent, EXISTS((member)-[:MEMBER_OF]->()-[:HOSTED_EVENT]->(futureEvent)) AS myGroup'
    cypher += ' OPTIONAL MATCH (member)-[:INTERESTED_IN]->()<-[:HAS_TOPIC]-()-[:HOSTED_EVENT]->(futureEvent)'
    cypher += ' WITH member, futureEvent, myGroup, COUNT(*) AS commonTopics'
    cypher += ' WHERE commonTopics >= 3'
    cypher += ' OPTIONAL MATCH (member)-[rsvp:RSVPD]->(previousEvent)<-[:HOSTED_EVENT]-()-[:HOSTED_EVENT]->(futureEvent)'
    cypher += ' WHERE previousEvent.time < timestamp()'
    cypher += ' WITH member, futureEvent, commonTopics, myGroup, COUNT(rsvp) AS previousEvents'
    cypher += ' MATCH (venue)<-[:VENUE]-(futureEvent)<-[:HOSTED_EVENT]-(group)'
    cypher += ' WITH member, futureEvent, group, venue, commonTopics, myGroup, previousEvents, distance(point(venue), point({latitude: 51.518698, longitude: -0.086146})) AS distance'
    cypher += ' OPTIONAL MATCH (member)-[rsvp:RSVPD]->(previousEvent)-[:VENUE]->(venue)'
    cypher += ' WHERE previousEvent.time < timestamp()'
    cypher += ' WITH futureEvent, group, venue, commonTopics, myGroup, previousEvents, distance, COUNT(previousEvent) AS eventsAtVenue'
    cypher += ' WITH futureEvent, group, venue, commonTopics, myGroup, previousEvents, distance, eventsAtVenue, CASE WHEN myGroup THEN 5 ELSE 0 END AS myGroupScore'
    cypher += ' WITH futureEvent, group, venue, commonTopics, myGroup, previousEvents, distance, eventsAtVenue, myGroupScore, round((futureEvent.time - timestamp()) / (24.0*60*60*1000)) AS days'
    cypher += ' RETURN futureEvent.name, futureEvent.time, group.name, venue.name, commonTopics, myGroup, previousEvents, days, distance, eventsAtVenue, myGroupScore + commonTopics + eventsAtVenue - days AS score'
    cypher += ' ORDER BY score DESC'

    print >> sys.stderr, "CYPHER = ", cypher
    result = session.run(cypher)

    for record in result:
        print >> sys.stderr, record

def calculate_score_for_events_within_500m_of_previously_visited_venues():

    cypher = "MATCH (member:Member) WHERE member.name CONTAINS 'Mark Needham'"
    cypher += ' MATCH (futureEvent:Event)'
    cypher += ' WHERE timestamp() + (7 * 24 * 60 * 60 * 1000) > futureEvent.time > timestamp()'

    cypher += ' WITH member, futureEvent, EXISTS((member)-[:MEMBER_OF]->()-[:HOSTED_EVENT]->(futureEvent)) AS myGroup'
    cypher += ' OPTIONAL MATCH (member)-[:INTERESTED_IN]->()<-[:HAS_TOPIC]-()-[:HOSTED_EVENT]->(futureEvent)'

    cypher += ' WITH member, futureEvent, myGroup, COUNT(*) AS commonTopics'
    cypher += ' WHERE commonTopics >= 3'
    cypher += ' OPTIONAL MATCH (member)-[rsvp:RSVPD]->(previousEvent)<-[:HOSTED_EVENT]-()-[:HOSTED_EVENT]->(futureEvent)'
    cypher += ' WHERE previousEvent.time < timestamp()'

    cypher += ' WITH member, futureEvent, commonTopics, myGroup, COUNT(rsvp) AS previousEvents'
    cypher += ' MATCH (venue)<-[:VENUE]-(futureEvent)<-[:HOSTED_EVENT]-(group)'

    cypher += ' WITH member, futureEvent, group, venue, commonTopics, myGroup, previousEvents, distance(point(venue), point({latitude: 51.518698, longitude: -0.086146})) AS distance'
    cypher += ' OPTIONAL MATCH (member)-[rsvp:RSVPD]->(previousEvent)-[:VENUE]->(aVenue)'
    # less than 500 m from previously visited venue
    cypher += ' WHERE previousEvent.time < timestamp() AND abs(distance(point(venue), point(aVenue))) < 500'

    cypher += ' WITH futureEvent, group, venue, commonTopics, myGroup, previousEvents, distance, COUNT(previousEvent) AS eventsAtVenue'
    cypher += ' WITH futureEvent, group, venue, commonTopics, myGroup, previousEvents, distance, eventsAtVenue, CASE WHEN myGroup THEN 5 ELSE 0 END AS myGroupScore'
    cypher += ' WITH futureEvent, group, venue, commonTopics, myGroup, previousEvents, distance, eventsAtVenue, myGroupScore, round((futureEvent.time - timestamp()) / (24.0*60*60*1000)) AS days'

    cypher += ' RETURN futureEvent.name, futureEvent.time, group.name, venue.name, commonTopics, myGroup, previousEvents, days, distance, eventsAtVenue, myGroupScore + commonTopics + eventsAtVenue - days AS score'
    cypher += ' ORDER BY score DESC'

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

    empty_db()

    # 1) Recommend groups by topic
    import_groups()
    if not dataImportOnly:
        show_groups()
    merge_and_constraints()
    if not dataImportOnly:
        show_constraints()
    import_topics()
    if not dataImportOnly:
        show_topics()
    connect_groups_and_topics()
    if not dataImportOnly:
        show_groups_and_topics()
    create_indexes_on_groups_and_topics()
    show_indices_on_groups_and_topics()
    
    # Exercises
    if not dataImportOnly:
        show_most_popular_topics()
        show_most_recently_created_group()
        show_groups_running_for_more_than_4_years()
        find_groups_with_neo4j_or_data_in_their_name()
        show_distinct_topics_for_these_groups()

    if not dataImportOnly:
        find_similar_groups_to_neo4j()
    
    # 2) Groups similar to mine
    if not dataImportOnly:
        explore_members()
    add_constraint_on_members()
    import_members()
    if not dataImportOnly:
        show_members()
    create_index_on_members()

    # Exercises
    if not dataImportOnly:
        find_some_member()
        find_member_of_how_many_groups()
        find_topics_of_these_groups()
        find_topics_that_show_up_most()

    if not dataImportOnly:
        exclude_groups_im_a_member_of()
        find_my_similar_groups()
        who_are_members_of_the_most_groups()
    
    # 3) My interests
    create_interested_in_relationship()
    if not dataImportOnly:
        find_my_similar_groups()

    # 4) Event recommendations
    create_constraints_and_indexes_on_events()
    import_events()
    if not dataImportOnly:
        show_events()
    connect_events_and_groups()
    if not dataImportOnly:
        show_groups_and_events()
        find_future_events_in_my_groups()

    # Layered recommendations
    if not dataImportOnly:
        find_future_events_for_my_topics()
    
    # Exercise:
    if not dataImportOnly:
        filter_out_events_which_have_less_than_3_common_topics()
        only_show_events_happening_in_the_next_7_days()
    
    if not dataImportOnly:
        sorting_the_events_by_some_importance_score()

    # 5) Venues
    create_constraint_on_venues()
    import_venues()
    connect_events_to_venues()
    if not dataImportOnly:
        verify_venues_import()
        find_venues_near_here()

    # Exercises
    if not dataImportOnly:
        update_recommender_to_also_return_distance_to_venue()
        filter_out_events_further_than_1km()

    # 6) RSVPs
    import_rsvps()
    include_attendance_at_previous_events_to_score()
    
    # Exercises
    if not dataImportOnly:
        calculate_score_for_events_at_previously_visited_venues()
        calculate_score_for_events_within_500m_of_previously_visited_venues()
