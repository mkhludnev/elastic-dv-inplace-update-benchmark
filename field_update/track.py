import random, json
from elasticsearch import Elasticsearch,helpers
from collections import defaultdict

words=[]
try:
     with open('/usr/share/dict/words', 'r') as f:
         for l in f :
            self._words.append(l.replace("\n",""))
except Exception as e:
    words +=['foo','bar','baz','lorem','ipsum','dolores',
             'moo','ban','crux','boom','greed','block']
    print(e)
    
def random_text(num_words):
    result=[]
    for i in range(0,num_words):
        result.append(random.choice(words))
    return result

def few_subscriptions(len, total):
    subscs = []
    for s in range(0, len):
        subscs.append(hex(random.randint(0, total)))
    return subscs
    
def get_random_search_parameters(track, params, **kwargs):
    default_index = "books"
    default_type = "books"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    _max_subs = int(params.get("subs-total", "1000"))
    return {
          "body":{
            "query": {
              "bool": {
                "must": {"match" : {"title": " ".join(random_text(2))}},
                "filter": { "term" : { "subscriptions": few_subscriptions(1,_max_subs)[0] } }
              }
            }
          },
        "index": index_name,
        "type": type_name
    }

def get_random_books_update_query(track, params, **kwargs):
    default_index = "books"
    default_type = "books"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    bulkSize = int(params.get("bulk-size", "100"))
    _max_subs = int(params.get("subs-total", "1000"))
    _num_subs = int(params.get("subs-per-book", "100"))
    body=""
    for x in range(0,bulkSize):
        book_id = random.randint(0,int(params["books-total"])-1)
        subscs= few_subscriptions(_num_subs, _max_subs)
        body+=(json.dumps({ "update" : {"_id" : book_id, "_type" : type_name, "_index" : index_name} })+'\n')
        body+=(json.dumps({ "doc" : { "subscriptions": subscs, "updated":True}})+'\n')
    output = {
        "body":body,
        "action-metadata-present":True,
        "bulk-size":bulkSize,
        "index":index_name,
        "type":type_name
    }
    return output


class InsertBooksSubsParamSource:
    def __init__(self, track, params, **kwargs):
        self._index_name = params.get("index", "books")
        self._type_name = params.get("type", "books")
        self._params = params
        self._bulk_size = int(params.get("bulk-size", "100"))
        self._max_subs = int(params.get("subs-total", "1000"))
        self._num_subs = int(params.get("subs-per-book", "100"))
        self._max_books = int(params.get("books-total", "10000000"))
    def partition(self, partition_index, total_partitions):
        return InsertBooksSubsClient(self,partition_index,total_partitions)

class InsertBooksSubsClient:
    def __init__(self, factory:InsertBooksSubsParamSource, partition_index, total_partitions):
        self._factory=factory
        self.partition_index=partition_index
        self.total_partitions=total_partitions
        self.iter = self.create_iter()

    def create_iter(self):
        """
        yield n-th bulk 
        """
        book_per_part=self._factory._max_books/self.total_partitions
        i=0
        body=""
        for book_id in range(int(book_per_part*self.partition_index), int(book_per_part*(self.partition_index+1))):
            subscs = few_subscriptions(self._factory._num_subs, self._factory._max_subs)
            body+=(json.dumps({ "index" : {"_id" : book_id, "_type" : self._factory._type_name, "_index" : self._factory._index_name} })+'\n')
            #{"title": "Book of minerals", "author": "Albertus, Magnus, Saint, 1193?-1280", "pubDate": "1967","subscriptions": ["CK","MA","AH"]}
            body+=(json.dumps({  
                "title": " ".join(random_text(4)),
                "author": " ".join(random_text(2)),
                "abstract": " ".join(random_text(100)),
                "pubDate": 1837+(i+51)%(2018-1837),
                "subscriptions": subscs})+'\n')
            i+=1
            if i%self._factory._bulk_size==0:
                b=body
                yield {
                    "body":b,
                    "action-metadata-present":True,
                    "bulk-size":self._factory._bulk_size,
                    "index":self._factory._index_name,
                    "type":self._factory._type_name
                }
                body=""
        if body!="":
            b=body
            yield {
                "body":b,
                "action-metadata-present":True,
                "bulk-size":bulkSize,
                "index":self._factory._index_name,
                "type":self._factory._type_name
            }
            body=""
        return
    def size(self):
        """"
        number of bulks per partition
        """
        return int(self._factory._max_books/self.total_partitions/self._factory._bulk_size)
    def params(self):
        return next(self.iter)

    #def random_text(self,num_words):
    #    result=[]
    #    for i in range(0,num_words):
    #        result.append(random.choice(self._factory._words))
    #    return result


def register(registry):
    registry.register_param_source("search-param-source", get_random_search_parameters)
    registry.register_param_source("update-param-source", get_random_books_update_query)
    registry.register_param_source("insert-books-subs-parallel",InsertBooksSubsParamSource)
