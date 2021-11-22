import csv
from elasticsearch import Elasticsearch
from elasticsearch import helpers
//from elasticsearch.serializer import JSONSerializer
import os



es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
print(es.ping())

def convert(filename,indexname,type):
    with open(filename, encoding="utf8") as file:
        r = csv.DictReader(file)
        helpers.bulk(es, r, index=indexname, doc_type=type)

es.indices.create(index = 'movies',ignore=400)
convert('movies.csv','movies','movies')

es.indices.create(index = 'ratings',ignore=400)
convert('ratings.csv','ratings','default')


#print(es.indices.exists('movies'))
