# Inspired by https://hub.docker.com/r/jgkim/neo4j/~/dockerfile/
FROM jfloff/alpine-python:2.7
MAINTAINER Frederik Durant <frederik.durant@pandora.be>

ADD neo4jloader/import_data.py /

RUN pip install neo4j-driver

CMD ["python", "./import_data.py"]
