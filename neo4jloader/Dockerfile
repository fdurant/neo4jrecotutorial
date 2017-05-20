# Inspired by https://hub.docker.com/r/jgkim/neo4j/~/dockerfile/
FROM python:2.7.13
MAINTAINER Frederik Durant <frederik.durant@pandora.be>

ADD neo4jloader/import_data.py /

# See https://github.com/neo4j/neo4j/issues/9233
RUN echo "deb http://httpredir.debian.org/debian/ jessie-backports main" >> /etc/apt/sources.list && \
  apt update && apt install -y -t jessie-backports openssl

RUN pip install neo4j-driver

CMD ["python", "./import_data.py"]
