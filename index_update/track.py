import random, json, string

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
    return "%s" % random.randint(0,9999)

def get_random_subscriptions_update_query(track, params, **kwargs):
    default_index = "subscriptions"
    default_type = "subscription"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    body=""
    for x in range(0,100):
        body+=(json.dumps({ "update" : {"_id" : "%s" % get_random_subscription(params), "_type" : type_name, "_index" : index_name} })+'\n')
        if random.randint(0,2)==0:
            body+=(json.dumps({ "script" : { "source": "ctx._source.books.add(params.book)", "lang" : "painless", "params" : {"book" : get_random_book_id(params)}}})+'\n')
        else:
            body+=(json.dumps({ "script" : { "source": "Random rand = new Random(); int booksSize = ctx._source.books.size(); ctx._source.books.remove(rand.nextInt(booksSize))", "lang" : "painless"}})+'\n')
    output = {
        "body":body,
        "action_metadata_present":"True",
        "bulk-size":100,
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
        "action_metadata_present":"True",
        "bulk-size":params["num_subscriptions"],
        "index":index_name,
        "type":type_name
    }
    return result

def insert_books_bulk_data(track, params, **kwargs):
    default_index = "books"
    default_type = "book"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    body="" 
    fp = open('books.json') 
    for x in range(0, 10000):
        body+=(json.dumps({ "create" : {"_id" : x, "_type" : type_name, "_index" : index_name } })+'\n')
        body+=(fp.readline()+'\n')
    fp.close()
    result = {
        "body":body,
        "action_metadata_present":"True",
        "bulk-size":10000,
        "index":index_name,
        "type":type_name
    }
    return result
    

def register(registry):
    registry.register_param_source("search-param-source", get_random_term_lookup_parameters)
    registry.register_param_source("update-param-source", get_random_subscriptions_update_query)
    registry.register_param_source("subscriptions-insert-param-source", insert_subscriptions_bulk_data)
    registry.register_param_source("books-insert-param-source", insert_books_bulk_data)


