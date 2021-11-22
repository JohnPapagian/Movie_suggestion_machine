
from elasticsearch import Elasticsearch
#es = Elasticsearch(http_compress=True)


es = Elasticsearch([{'host': 'localhost', 'port': 9200}])



query = input("Insert a keyword:\n")
results = es.search(
    index='movies',
    body={
        'query':{
            "function_score": {
                "query": {
                    "match": {
                        "title": query
                    }
                }
            }
        },
    }
)

for line in results['hits']['hits']:
    print(line['_source'],line['_score'],"\n")
    print(line['_source']['genres'])

print("\n\n END OF PROGRAM \n\n")
