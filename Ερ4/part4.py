from elasticsearch import Elasticsearch
import gensim
from gensim.models import Word2Vec


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
word="Star Trek: The Motion Picture (1979)"

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



#gather all movie titles in list
titles=[]
for line in all['hits']['hits']:
         titles.append(line['_source']['title'])


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
#remove stopwords




#Split with a char that doent exist
#So the titles are split in list of lists-compatable with
#gensim-Word2Vec
i=0

for id in range(len(titles)):
    titles[id]=remove_stop_words(titles[id])


for string in titles:
    titles[i]=titles[i].split("|")
    i+=1



#gather genres of movies
allgens=[]
for line in all['hits']['hits']:
    allgens.append(line['_source']['genres'].split("|"))


#create title vectors
model = Word2Vec(sentences=titles,size=20, #create vectors
window=5, min_count=1)
#train model so similar titles get similar vectors
model.train(titles, total_examples=model.corpus_count, #train vectors
epochs=5)



#final contains movie titles and their genres
final=[j for i in zip(titles,allgens) for j in i]
final=zip(allgens,titles)

#create one-hot represantation for genres
i=0
onehot={}
for gen in gens:
    onehot[gen]=[0]*20
    onehot[gen][i]+=1
    i+=1

for title in final:
      tmpgen=[0]*20
      for gen in title[0]:
          tmpgen=[x+y for x,y in zip(tmpgen,onehot[gen])]


      model.wv[title[1]]+=[i*0.6 for i in tmpgen]




userId = input('Enter your userId:')
keyword = input('Enter a keyword:')




#Search for movie titles containing the keyword


result1 = es.search( #search movie titles
    index='movies',
    body={
        "query":{
         "match"
            :{

             "title": userId   }}}

            )
rtable=[]*2
maxsim=0

for line in result1['hits']['hits']:


#for each movie check if the user has seen it(in "ratings")

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


    if(result2['hits']['hits']): #if he has seen it-It's ok
        rtable.append([line['_source']['title'],float(line3['_source']['rating'])])
    else:
        #try to fill in a rating code below


        #what movies has the user seen-get rating
        result3=es.search(

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
        }
        )

        for line3 in result3['hits']['hits']:

            #what is the movies title
            result4=es.search(

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


        score=line['_score']+maxsim*float(line3['_source']['rating'])
        rtable.append([line['_source']['title'],score])





def func(table):
    return table[1]
rtable.sort(key=func,reverse=True)

print("\n\n")
for movie in rtable:
    print(movie)
