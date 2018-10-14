import random, json
from elasticsearch import Elasticsearch,helpers
from collections import defaultdict

subscriptions_to_books_mapping=defaultdict(list)

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
    bulkSize = params.get("bulk_size", "100")
    body=""
    for x in range(0,int(bulkSize)):
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

def register(registry):
    registry.register_param_source("search-param-source", get_random_search_parameters)
    registry.register_param_source("update-param-source", get_random_books_update_query)

