import random, json, string
from elasticsearch import Elasticsearch,helpers


def get_random_term_lookup_parameters(track, params, **kwargs):
    default_index = "books"
    default_type = "book"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    return {
        "body":{
            "query": {
                "bool": {
                    "must": {
                        "match" : {"title": get_random_book_title(params)}
                    },
                    "filter":{
                        "terms": {
                            "_id": {
                                "index": "subscriptions",
                                "type": "subscription",
                                "id": get_random_subscription(params),
                                "path": "books"
                            }
                        }       
                    }
                }
            }
        },
        "index": index_name
    }
def get_random_book_title(params):
    return "%s" % random.choice(params["titles"])

def get_random_subscription(params):
    return "%s" % random.choice(params["subscriptions"])

def get_random_book_id(params):
    return "%s" % random.randint(0,params["num_ids"])

def get_random_subscriptions_update_query(track, params, **kwargs):
    default_index = "subscriptions"
    default_type = "subscription"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    body=""
    bulkSize = 1500
    books_nums=[]
    for x in range(0,bulkSize):
        books_nums+=[get_random_book_id(params)];
    body+=(json.dumps({ "update" : {"_id" : "%s" % get_random_subscription(params), "_type" : type_name, "_index" : index_name} })+'\n')
    body+=(json.dumps({ "books" : books_nums})+'\n')
    output = {
        "body":body,
        "action-metadata-present":"True",
        "bulk-size":bulkSize,
        "index":index_name,
        "type":type_name
    }
    return output
    
def insert_subscriptions_bulk_data(track, params, **kwargs):
    default_index = "subscriptions"
    default_type = "subscription"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    body="" 
    fp = open('subscriptionslist.json') 
    for x in string.ascii_uppercase:
        for y in string.ascii_uppercase:
            body+=(json.dumps({ "create" : {"_id" : x+""+y, "_type" : type_name, "_index" : index_name } })+'\n')
            body+=(fp.readline()+'\n')
    fp.close()
    result = {
        "body":body,
        "action-metadata-present":"True",
        "bulk-size":params["num_subscriptions"],
        "index":index_name,
        "type":type_name
    }
    return result

def insert_books_bulk_data(track, params, **kwargs):
    es = Elasticsearch("127.0.0.1:39200")
    default_index = "books"
    default_type = "book"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    data=[] 
    fp = open('books.json') 
    for x in range(0, params["num_ids"]):
        data.append({ "_id" : x, "_type" : type_name, "_index" : index_name,"_op_type": "create","_source":json.loads(fp.readline())})
    fp.close()
    helpers.bulk(es, data)
    result = {
        "body":"false",
        "action-metadata-present":"True",
        "bulk-size":10,
        "index":index_name,
        "type":type_name
    }
    return result
    

def register(registry):
    registry.register_param_source("search-param-source", get_random_term_lookup_parameters)
    registry.register_param_source("update-param-source", get_random_subscriptions_update_query)
    registry.register_param_source("subscriptions-insert-param-source", insert_subscriptions_bulk_data)
    registry.register_param_source("books-insert-param-source", insert_books_bulk_data)


