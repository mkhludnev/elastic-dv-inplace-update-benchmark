import random, json
from elasticsearch import Elasticsearch,helpers
from collections import defaultdict

def get_random_search_parameters(track, params, **kwargs):
    default_index = "books"
    default_type = "books"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    return {
          "body":{
            "query": {
              "bool": {
                "must": {"match" : {"title": get_random_book_title(params)}},
                "filter": { "term" : { "subscriptions": get_random_subscription(params) } }
              }
            }
          },
        "index": index_name,
        "type": type_name
    }

def get_random_book_title(params):
    return "%s" % random.choice(params["titles"])

def get_random_subscription(params):
    return "%s" % random.choice(params["subscriptions"])

def get_random_book_id(params):
    return "%10d" % random.randint(0,params["num_ids"])

def get_random_books_update_query(track, params, **kwargs):
    default_index = "books"
    default_type = "books"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    bulkSize = int(params.get("bulk_size", "100"))
    body=""
    for x in range(0,bulkSize):
        book_id = get_random_book_id(params)
        subscs=[]
        for s in range(0,5):
            subscs+=[get_random_subscription(params)]
        body+=(json.dumps({ "update" : {"_id" : "%s" % book_id, "_type" : type_name, "_index" : index_name} })+'\n')
        body+=(json.dumps({ "doc" : { "subscriptions": subscs, "updated":True}})+'\n')
    output = {
        "body":body,
        "action-metadata-present":True,
        "bulk-size":bulkSize,
        "index":index_name,
        "type":type_name
    }
    return output

def insert_books_with_subscription_closure(words):
    def random_text( num_words):
        result=[]
        for i in range(0,num_words):
            result.append(random.choice(words))
        return
    count=0
    def insert_books(track, params, **kwargs):
        nonlocal count
        index_name = params.get("index", "books")
        type_name = params.get("type",  "books")
        bulkSize = int(params.get("bulk-size", "100"))
        maxSubs = int(params.get("subs-total", "10000"))
        numSubs = int(params.get("subs-per-book", "100"))
        body=""
        for x in range(0,bulkSize):
            book_id = "%d"%count
            count+=1
            subscs=[]
            for s in range(0,numSubs):
               subscs.append(hex(random.randint(0,maxSubs)))
            body+=(json.dumps({ "index" : {"_id" : book_id, "_type" : type_name, "_index" : index_name} })+'\n')
            #{"title": "Book of minerals", "author": "Albertus, Magnus, Saint, 1193?-1280", "pubDate": "1967","subscriptions": ["CK","MA","AH"]}
            body+=(json.dumps({ "doc" : { 
                "title": random_text(4),
                "author": random_text(2),
                "abstract": random_text(100),
                "pubDate": 1837+(count+51)%(2018-1837),
                "subscriptions": subscs, 
                "updated":True}})+'\n')
        output = {
             "body":body,
             "action-metadata-present":True,
             "bulk-size":bulkSize,
             "index":index_name,
             "type":type_name
        }
        #print(json.dumps(output))
        return output
    return insert_books

def register(registry):
    registry.register_param_source("search-param-source", get_random_search_parameters)
    registry.register_param_source("update-param-source", get_random_books_update_query)
    words=[]
    try:
         with open('/usr/share/dict/words', 'r') as f:
             for l in f :
                words.append(l)
    except Exception as e:
        words +=['foo','bar','baz','lorem','ipsum','dolores','moo','ban','crux','boom','greed','block']       
        print(e)
    registry.register_param_source("insert-books-subscription",insert_books_with_subscription_closure(words))
