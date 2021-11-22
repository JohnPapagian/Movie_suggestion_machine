from elasticsearch import Elasticsearch
from sklearn.cluster import KMeans
import gensim
import tensorflow as tf
from gensim.models import Word2Vec
import numpy as np
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
def findAllCats(all):

    for line in all['hits']['hits']:
        txt=line['_source']['genres'].split("|")
        for word in txt:
            flag=0
            for gen in arr:
                if(word==gen):
                     flag=1
        if(flag==0):
             arr.append(word)
    return arr
gens=findAllCats(all)


def remove_stop_words(text):
    stop_words = ['is', 'a', 'will', 'be','the','A','The']
    results = []
    #print(text)
    tmp = "".join(text).split(' ')
    for stop_word in stop_words:
        if stop_word in tmp:
            tmp.remove(stop_word)
    #results.append(" ".join(tmp))
    results=' '.join(tmp)
    return results


##########################################################
##############  Cluster work   ###########################
##########################################################
rdict={word:[0.0,0.0] for word in gens} #Make dictionaries
tempArr=[]
tempArr = [i for i in range(671)]

Rdict={key:copy.deepcopy(rdict) for key in tempArr }


i=1 # for each user make a list of average genre ratings
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
        },size=100
        )

    for line in results32['hits']['hits']: #for each movie note the ratings


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
            },size=100
            )

        for line5 in results33['hits']['hits']:
            txt=line5['_source']['genres'].split("|")
            for word in txt:
                Rdict[i][word][0]+=float(line['_source']['rating']) #add rating to sum
                Rdict[i][word][1]+=1 #increase count

    i+=1



i=0
for i in Rdict: #calculate number and sum of ratings
    for word in gens:
        if (Rdict[i][word][1]!=0):
            Rdict[i][word][0]=Rdict[i][word][0]/Rdict[i][word][1]



i=0
k=0


w, h = 20, 671;
ktable = [[0.0 for x in range(w)] for y in range(h)]

for user in Rdict: #transform data to 2d table

    k=0
    while(k<20):
        for gen in gens:
            #print(ktable[user][k])
            ktable[user][k]=float(Rdict[user][gen][0])

            k+=1


kmeans = KMeans(n_clusters=3)
kmeans.fit(ktable)

#END OF CLUSTER WORK



##########################################################
##############  Neural network   #########################
##########################################################

#gather all movie titles in list
titles=[]
for line in all['hits']['hits']:
         titles.append(line['_source']['title'])





for id in range(len(titles)):
    titles[id]=remove_stop_words(titles[id])


#Split titles
#So the titles are split in list of lists-compatable with
#gensim-Word2Vec
i=0
for string in titles:
    titles[i]=titles[i].split("',")
    i+=1



#gather genres of movies
allgens=[]
for line in all['hits']['hits']:
    allgens.append(line['_source']['genres'].split("|"))


#create title vectors
model = Word2Vec(sentences=titles,size=20, #create vectors
window=5, min_count=1)



#ntable contains movie titles and their genres
ntable=[j for i in zip(titles,allgens) for j in i]
ntable=zip(allgens,titles)

#create one-hot represantation for genres
i=0
onehot={}
for gen in gens:
    onehot[gen]=[0]*20
    onehot[gen][i]+=1
    i+=1

for title in ntable:
      tmpgen=[0]*20
      for gen in title[0]:
          tmpgen=[x+y for x,y in zip(tmpgen,onehot[gen])]

      model.wv[title[1]]+=[i*0.6 for i in tmpgen]

#END OF NEURAL NETWORK WORK






##########################################################
##############  Query Code   #############################
##########################################################

userId = input('Enter your userId:')
keyword = input('Enter a keyword:')


#Search for movie titles containing the keyword


result1 = es.search( #search movie titles
    index='movies',
    body={
        "query":{
         "match"
            :{

             "title": keyword   }}}

            )

maxsim=0
BM25table=[]        #avg movie rating table
results_fin=[]*3    #final results table
ClRatings=[]*2      #cluster ratings
i=0

#for each movie check if the user has seen it(in "ratings")
for line in result1['hits']['hits']:
    result2=es.search(

      index='ratings',
      body={
      "query": {
        "bool": {
          "must": [
            {
              "match": {
                "movieId": line['_source']['movieId']
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


    if(result2['hits']['hits']): #if the user has seen it-Use the rating
        results_fin.append([line['_source']['title'],float(line3['_source']['rating'])])
    else:



        #IF the user hasn't seen the movie
        #compute a score based on 4 variables
        #BM25 Score, Avg movie rating
        #Cluster made from similar users
        #and a neural network

        #get BM25 score first
        BM25table.append(line['_score'])


        #get movie average score second

        resultavg=es.search(index='ratings', body={
         "query": {
           "bool": {
            "must": {
                 "match": {
                   "movieId":  line['_source']['movieId']
                 }
               }
             }
           }
         }, size=25
         )

        #for each result get the rating-so later we can get the average
        for lineavg in resultavg['hits']['hits']:
             num+=1
             k=float(lineavg['_source']['rating'])
             sum+= k

        if(num):
            avg=sum/num #avg rating for the movie
        else:
            sum=0.0



          #Get the cluster rating 3nd

        rating1=line['_source']['genres'].split('|')
        cnt=0
        flag=0
        #compare all movies gens with possible genres
        for gen in gens:
            for mgen in rating1:
                if(gen==mgen):
                    flag=1
            if(flag==1):
                break
            cnt+=1

        if(cnt==20):
            cnt-=1
        ClRatings.append(ktable[user][cnt])




        #Finally get the neural network similarity score(keyword-title simiilarity)
        result3=es.search( #what movies has the user seen-get rating

          index='ratings',
          body={
          "query": {
            "bool": {
              "must": [
                {
                  "match": {
                    "userId": userId
                  }
                }
              ]
            }
          }
        },size=40
        )
        for line3 in result3['hits']['hits']:

            result4=es.search( #what is the movies title

              index='movies',
              body={
              "query": {
                "bool": {
                  "must": [
                    {
                      "match": {
                        "movieId": line3['_source']['movieId']
                      }
                    }
                  ]
                }
              }
            }
            )

            for movie in result4['hits']['hits']:
                tmp1=remove_stop_words(movie['_source']['title'])
                tmp2=remove_stop_words(line['_source']['title'])
                x=model.wv.similarity(tmp1,tmp2)
                if(x>maxsim):
                    maxsim=x





        #gather AI res+cluster res+avg rating res+BM25 similarity res
        tmp=(maxsim*float(line3['_source']['rating'])+ClRatings[i]+avg+BM25table[i])
        results_fin.append([line['_source']['title'],tmp])
        i+=1


def func(arr1):
    return arr1[1]


#/sort and print results
results_fin.sort(key=func,reverse=True)

print("\n\nMovie title    Score")
for movie in results_fin:
    print(movie)
