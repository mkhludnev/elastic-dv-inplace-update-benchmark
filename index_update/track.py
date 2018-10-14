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
        "index": index_name,
        "type": type_name
    }
def get_random_book_title(params):
    return "%s" % random.choice(params["titles"])

def get_random_subscription(params):
    return "%s" % random.choice(params["subscriptions"])

def get_random_book_id(params):
    return "%10d" % random.randint(0,params["num_ids"])

def get_random_subscriptions_update_query(track, params, **kwargs):
    default_index = "subscriptions"
    default_type = "subscription"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    body=""
    books_upd = int(params.get("books-to-update", "1500"))
    books_nums=[]
    for x in range(0,books_upd):
        books_nums+=[get_random_book_id(params)];
    body+=(json.dumps({ "update" : {"_id" : "%s" % get_random_subscription(params), "_type" : type_name, "_index" : index_name} })+'\n')
    body+=(json.dumps({"doc":{ "books" : books_nums, "updated":True}})+'\n')
    output = {
        "body":body,
        "action-metadata-present":True,
        "bulk-size":1,
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
    count=0
    fp = open('subscriptionslist.json') 
    for x in string.ascii_uppercase:
        for y in string.ascii_uppercase:
            body+=(json.dumps({ "create" : {"_id" : x+""+y, "_type" : type_name, "_index" : index_name } })+'\n')
            books_object = json.loads(fp.readline())
            paddedarr=["%10d"%i for i in books_object["books"]]
            body+=(json.dumps({"books":paddedarr})+'\n')
            count+=1
    fp.close()
    result = {
        "body":body,
        "action-metadata-present":True,
        "bulk-size":count,
        "index":index_name,
        "type":type_name
    }
    return result

def register(registry):
    registry.register_param_source("search-param-source", get_random_term_lookup_parameters)
    registry.register_param_source("update-param-source", get_random_subscriptions_update_query)
    registry.register_param_source("subscriptions-insert-param-source", insert_subscriptions_bulk_data)


