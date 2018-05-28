import random, json
from elasticsearch import Elasticsearch
from elasticsearch import helpers


def get_random_search_parameters(track, params, **kwargs):
    default_index = "books"
    default_type = "book"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    return {
          "body":{
            "query": {
              "bool": {
                "must": { "match_all" : { }},
                 "filter": [
                    { "term" : { "title": get_random_book_title(params) } },
                    { "term" : { "subscriptions": get_random_subscription(params) } }
                  ]
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
    return "%s" % random.randint(0,999)#params["num_ids"])

def get_random_books_update_query(track, params, **kwargs):
    es = Elasticsearch("127.0.0.1:39200")
    default_index = "books"
    default_type = "book"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    subscription = get_random_subscription(params)
    output = [
        {
            "_index": index_name,
            "_type": type_name,
            "_id": get_random_book_id(params),
            "_op_type": "update",
            "script" : { "source": "ctx._source.subscriptions.add('DF')"}
        }
        for x in range(0, 100)
    ]
        #output.append({ "update" : {"_id" : "%s" % get_random_book_id(params), "_type" : type_name, "_index" : index_name, "retry_on_conflict" : 1} })
        #output.append({ "script" : { "source": "ctx._source.subscriptions.add[parameters.subscription]", "lang" : "painless", "parameters" : {"subscription" : subscription }}})
    #output = {
    #    "body":"\n".join(map(json.dumps,output)),
    #    "path":"/_bulk",
    #    "method":"POST",
    #    "headers":"Content-Type: application/json"
    #}
    
    helpers.bulk(es, output)
    result = {
        "body":"{}",
        "path":"/_bulk",
        "method":"POST",
        "headers":"Content-Type: application/json",
        "action_metadata_present":"False",
        "bulk-size":"1"
    }
    return result
    
def insert_bulk_data(track, params, **kwargs):
    es = Elasticsearch("127.0.0.1:39200")
    default_index = "books"
    default_type = "book"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    try:  
        fp = open('books.json') 
        output = [
            {
                "_index": index_name,
                "_type": type_name,
                "_id": x,
                "_op_type": "create",
                "_source": json.loads(fp.readline())
            }
            for x in range(0, 1000)#params["num_ids"])
           
        ]

    finally:  
        fp.close()
    helpers.bulk(es, output)
    result = {
        "body":"{}",
        "path":"/_bulk",
        "method":"POST",
        "headers":"Content-Type: application/json",
        "action_metadata_present":"False",
        "bulk-size":"1"
    }
    return result
        #output.append({ "update" : {"_id" : "%s" % get_random_book_id(params), "_type" : type_name, "_index" : index_name, "retry_on_conflict" : 1} })
        #output.append({ "script" : { "source": "ctx._source.subscriptions.add[parameters.subscription]", "lang" : "painless", "parameters" : {"subscription" : subscription }}})
    #output = {
    #    "body":"\n".join(map(json.dumps,output)),
    #    "path":"/_bulk",
    #    "method":"POST",
    #    "headers":"Content-Type: application/json"
    #}
    

def register(registry):
    registry.register_param_source("search-param-source", get_random_search_parameters)
    registry.register_param_source("update-param-source", get_random_books_update_query)
    registry.register_param_source("insert-param-source", insert_bulk_data)

