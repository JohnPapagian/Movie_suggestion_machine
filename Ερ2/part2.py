
from elasticsearch import Elasticsearch



#es = Elasticsearch(http_compress=True)
arr=[]
num=0
avg=0.0


es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

#Get the userId and a keyword to search for
userId = input('Enter your userId:')
keyword = input('Enter a keyword:')




#Search for movie titles containing the keyword
Data1 = es.search( #search movie titles
    index='movies',
    body={
        "query":{
         "match"
            :{

             "title": keyword   }}},size=100

            )

#for each movie found
#//search for the users rating for the movie
for res1 in Data1['hits']['hits']:

    Data2=es.search(
      index='ratings',
      body={
        "query": {
            "bool": {
                "must": [
                {
                    "match": {
                        "movieId": res1['_source']['movieId']
                        }
                },
            {
                "match": {
                "userId": userId
                        }
              }
            ]
          }
      }
    }
    )
    #if the user has seen the movie-get the rating so we can use it later
    if(Data2['hits']['hits']):
            for res2 in Data2['hits']['hits']:
                val=float(res2['_source']['rating'])

    else:
        #else userRating=val=0
        val=0.00



    #search for each movies ratings-so we can calculate the average
    Data3=es.search(
    index='ratings', body={
     "query": {
       "bool": {
        "must": {
             "match": {
               "movieId":  res1['_source']['movieId']
             }
           }
         }
       }
     }, size=100
     )

    #for each result get the rating-so later we can get the average
    for res3 in Data3['hits']['hits']:
         num+=1
         k=float(res3['_source']['rating'])
         avg+= k


    #get average
    if(num):
        avg=avg/num

    else:
        avg=0.0
    num=0

    #calculate the result->(score+average rating+user rating)/3
    result=(float(res1['_score'])+avg+val)/3
    x=(res1['_source']['title'],result)
    arr.append(x)



def func(arr1):
    return arr1[1]

#/sort and print Data
arr.sort(key=func,reverse=True)
for res in arr:
    print(res)



print("\n\n END OF PROGRAM \n\n")
