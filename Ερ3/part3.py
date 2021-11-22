
from elasticsearch import Elasticsearch
from sklearn.cluster import KMeans
import copy



arr=[]
arr1=[]
num=0
sum=0.0



es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

all = es.search( #search movie genres
    index='movies',
    body={
        "query":{
         "match_all":{}}
         },size=9126
            )


flag=0

def findAllCats(data):

    for line in data['hits']['hits']:
        txt=line['_source']['genres'].split("|")
        for word in txt:
            flag=0
            for gen in arr:
                #print(word)
                if(word==gen):
                     flag=1
        if(flag==0):
             #print("\nim appending dis:",word)
             arr.append(word)
    return arr

gens=findAllCats(all)





##########################################################
##############  Cluster work   ###########################
##########################################################

#Data will be gathered in a dictionary of dictionaries
#Than it will be transfered to a 2d-list for further procesing

rdict={word:[0.0,0.0] for word in gens}
tempArr=[]
tempArr = [i for i in range(671)]

Rdict={key:copy.deepcopy(rdict) for key in tempArr }


# for each user make a list of average genre ratings
i=1
while(i<671):
    results32=es.search(

    index='ratings',
    body={
    "query": {
            "bool": {
              "must": [
                              {
                  "match": {
                    "userId": i
                  }
                }
              ]
            }
             }
        }
        )



        #for each movie note the ratings
    for line in results32['hits']['hits']:


        results33=es.search(
        index='movies',
        body={
        "query": {
                "bool": {
                  "must": [
                                  {
                      "match": {
                        "movieId": line['_source']['movieId']
                      }
                    }
                  ]
                }
                 }
            }
            )

        for line5 in results33['hits']['hits']:
            txt=line5['_source']['genres'].split("|")

            for word in txt:

                #add rating to sum
                Rdict[i][word][0]+=float(line['_source']['rating'])

                #increase count
                Rdict[i][word][1]+=1

    i+=1




#calculate number and sum of ratings
i=0
for i in Rdict:
    for word in gens:
        if (Rdict[i][word][1]!=0):
            Rdict[i][word][0]=Rdict[i][word][0]/Rdict[i][word][1]




i=0
k=0
for gen in gens:
    i+=1

w, h = i, 671;


#create 2d table-20*671
ktable = [[0.0 for x in range(w)] for y in range(h)]


#transform data to 2d table
for user in Rdict: #
    k=0
    while(k<i):
        for gen in gens:
            ktable[user][k]=float(Rdict[user][gen][0])
            k+=1

#Create clusters
kmeans = KMeans(n_clusters=2)
kmeans.fit(ktable)



#for each user/for each genre-ktable holds the average rating for the gen
#so if for example:user 1 has not rated a movie-and the movie belongs to
#the action genre ,we can get the avg cluster rating for the movie

for i in range(671):
    for j in range(20):
        if(ktable[i][j]==0):
            ktable[i][j]=kmeans.cluster_centers_[kmeans.labels_[i]][j]




#Search for movie titles containing the keyword
userId=int(input("Insert your user id "))
keyword= input('Insert a keyword: ')




pin=[]


res = es.search(index='movies', body={ #query keyword
  "query": {
        "match": {
          "title": keyword
        }
      }
    }, size=50)

cnt=0
#for each result create the new score
for doc in res['hits']['hits']:
     res1 = es.search(index='ratings', body={
       "query": {
         "bool": {
           "must": [
             {
               "match": {
                 "movieId": doc['_source']['movieId']
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
     }, size=50)
     #if the user has seen it -rating=movie rating
     if(res1['hits']['hits']):
         for doc1 in res1['hits']['hits']:
              rating=float(doc1['_source']['rating'])
     #else rating =cluster average for the 1st category we match
     else:
        rating=0.0
        rating1=doc['_source']['genres'].split('|')

        cnt=0
        flag=0
        for gen in gens:
            for mgen in rating1:
                if(gen==mgen):
                    flag=1
            if(flag==1):
                break
            cnt+=1

        if(cnt==20):
            cnt-=1

        rating=ktable[userId][cnt]



     cnt=0
     sum=0.0
     res2 = es.search(index='ratings', body={
       "query": {
         "bool": {
           "must": {
             "match": {
               "movieId":  doc['_source']['movieId']
             }
           }
         }
       }
     }, size=100)
     
     #avg movie rating-from part 2 of project
     for doc2 in res2['hits']['hits']:
         cnt+=1
         x=float(doc2['_source']['rating'])
         sum+= x
     if(cnt):
        sum=sum/cnt
     else:
        sum=0.0


     #similarity score+avg movie rating+avg rating for cluster
     result=(float(doc['_score'])+sum+rating)/3
     y=(doc['_source']['title'],result)
     pin.append(y)

def func(table):
    return table[1]

pin.sort(key=func,reverse=True)

for i in pin:
    print(i)



print("\n\n END OF PROGRAM \n\n")
