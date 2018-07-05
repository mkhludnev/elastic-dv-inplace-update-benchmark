import random, json
from elasticsearch import Elasticsearch,helpers

def get_random_search_parameters(track, params, **kwargs):
    default_index = "books"
    default_type = "book"
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
    return "%s" % random.randint(0,params["num_ids"])

def get_random_books_update_query(track, params, **kwargs):
    default_index = "books"
    default_type = "book"
    operations = ["add","remove"]
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    subscription = get_random_subscription(params)
    bulkSize = 100
    body=""
    for x in range(0,bulkSize):
        body+=(json.dumps({ "update" : {"_id" : "%s" % get_random_book_id(params), "_type" : type_name, "_index" : index_name} })+'\n')
        if random.randint(0,2)==0:
            body+=(json.dumps({ "script" : { "source": "ctx._source.subscriptions.add(params.subscription)", "lang" : "painless", "params" : {"subscription" : subscription }}})+'\n')
        else:
            body+=(json.dumps({ "script" : { "source": "Random rand = new Random(); int subscriptionsSize = ctx._source.subscriptions.size(); ctx._source.subscriptions.remove(rand.nextInt(subscriptionsSize))", "lang" : "painless"}})+'\n')
    output = {
        "body":body,
        "action_metadata_present":"True",
        "bulk-size":bulkSize,
        "index":index_name,
        "type":type_name
    }
    return output
    
def insert_bulk_data(track, params, **kwargs):
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
        "action_metadata_present":"True",
        "bulk-size":10,
        "index":index_name,
        "type":type_name
    }
    return result
    

def register(registry):
    registry.register_param_source("search-param-source", get_random_search_parameters)
    registry.register_param_source("update-param-source", get_random_books_update_query)
    registry.register_param_source("insert-param-source", insert_bulk_data)

