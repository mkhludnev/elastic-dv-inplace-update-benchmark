import random, json
from elasticsearch import Elasticsearch,helpers
from collections import defaultdict

primes=[
    997,1009,1013,
   1019,1021,1031,1033,1039,1049,1051,1061,1063,1069,1087,1091,1093,1097,1103,1109,1117,1123,1129,1151,
   1153,1163,1171,1181,1187,1193,1201,1213,1217,1223,1229,1231,1237,1249,1259,1277,1279,1283,1289,1291,
   1297,1301,1303,1307,1319,1321,1327,1361,1367,1373,1381,1399,1409,1423,1427,1429,1433,1439,1447,1451,
   1453,1459,1471,1481,1483,1487,1489,1493,1499,1511,1523,1531,1543,1549,1553,1559,1567,1571,1579,1583,
   1597,1601,1607,1609,1613,1619,1621,1627,1637,1657,1663,1667,1669,1693,1697,1699,1709,1721,1723,1733,
   1741,1747,1753,1759,1777,1783,1787,1789,1801,1811,1823,1831,1847,1861,1867,1871,1873,1877,1879,1889,
   1901,1907,1913,1931,1933,1949,1951,1973,1979,1987,1993,1997,1999,2003,2011,2017,2027,2029,2039,2053,
   2063,2069,2081,2083,2087,2089,2099,2111,2113,2129,2131,2137,2141,2143,2153,2161,2179,2203,2207,2213,
   2221,2237,2239,2243,2251,2267,2269,2273,2281,2287,2293,2297,2309,2311,2333,2339,2341,2347,2351,2357,
   2371,2377,2381,2383,2389,2393,2399,2411,2417,2423,2437,2441,2447,2459,2467,2473,2477,2503,2521,2531,
   2539,2543,2549,2551,2557,2579,2591,2593,2609,2617,2621,2633,2647,2657,2659,2663,2671,2677,2683,2687,
   2689,2693,2699,2707,2711,2713,2719,2729,2731,2741,2749,2753,2767,2777,2789,2791,2797,2801,2803,2819,
   2833,2837,2843,2851,2857,2861,2879,2887,2897,2903,2909,2917,2927,2939,2953,2957,2963,2969,2971,2999,
   3001,3011,3019,3023,3037,3041,3049,3061,3067,3079,3083,3089,3109,3119,3121,3137,3163,3167,3169,3181,
   3187,3191,3203,3209,3217,3221,3229,3251,3253,3257,3259,3271,3299,3301,3307,3313,3319,3323,3329,3331,
   3343,3347,3359,3361,3371,3373,3389,3391,3407,3413,3433,3449,3457,3461,3463,3467,3469,3491,3499,3511,
   3517,3527,3529,3533,3539,3541,3547,3557,3559,3571,3581,3583,3593,3607,3613,3617,3623,3631,3637,3643,
   3659,3671,3673,3677,3691,3697,3701,3709,3719,3727,3733,3739,3761,3767,3769,3779,3793,3797,3803,3821,
   3823,3833,3847,3851,3853,3863,3877,3881,3889,3907,3911,3917,3919,3923,3929,3931,3943,3947,3967,3989]
   
words=[]
try:
     with open('/usr/share/dict/words', 'r') as f:
         for l in f :
            words.append(l.replace("\n",""))
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

def get_random_searchDV_parameters(track, params, **kwargs):
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
                "filter": { "term" : { "subscription_"+few_subscriptions(1,_max_subs)[0]:1 } }
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
    def __init__(self, factory:InsertBooksSubsParamSource, partition_index, total_partitions,
                 index_subs=True):
        self._factory=factory
        self.partition_index=partition_index
        self.total_partitions=total_partitions
        self.iter = self.create_iter()
        self.index_subs=index_subs

    def create_iter(self):
        """
        yield n-th bulk 
        """
        book_per_part=self._factory._max_books/self.total_partitions
        i=0
        body=""
        for book_id in range(int(book_per_part*self.partition_index), int(book_per_part*(self.partition_index+1))):
            body+=(json.dumps({ "index" : {"_id" : book_id, "_type" : self._factory._type_name, "_index" : self._factory._index_name} })+'\n')
            #{"title": "Book of minerals", "author": "Albertus, Magnus, Saint, 1193?-1280", "pubDate": "1967","subscriptions": ["CK","MA","AH"]}
            
            d = {  
                "title": " ".join(random_text(4)),
                "author": " ".join(random_text(2)),
                "abstract": " ".join(random_text(100)),
                "pubDate": 1837+(i+51)%(2018-1837)
                }
            if self.index_subs :
                subscs = few_subscriptions(self._factory._num_subs, self._factory._max_subs)
                d["subscriptions"]=subscs
                for s in subscs:
                    d["subscription_"+s]=1 
            body+=(json.dumps(d)+'\n')
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
                "bulk-size":self._factory._bulk_size,
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

class InsertBooksOnly(InsertBooksSubsParamSource):
    def partition(self, partition_index, total_partitions):
        return InsertBooksSubsClient(self,partition_index,total_partitions, index_subs=False)

class InsertSubsOnly:
    def __init__(self, track, params, **kwargs):
        self._params=params
    def partition(self, partition_index, total_partitions):
        return InsertSubsClient(self,partition_index,total_partitions)

class InsertSubsClient:
    def __init__(self, factory:InsertSubsOnly, partition_index, total_partitions):
        self._factory=factory
        self.partition_index=partition_index
        self.total_partitions=total_partitions
        self.bulk_size = int(self._factory._params.get("bulk-size", "100"))
        self._index_name=self._factory._params.get("index", "subscriptions")
        self._type_name=self._factory._params.get("type", "subscriptions")
        self.books_per_subs = int(self._factory._params.get("books-per-sub", "1000"))
        self.books_total = int(self._factory._params.get("books-total", "100000"))
        self.subs_total = int(self._factory._params.get("subs-total","2000"))
        self.iter = self.create_iter()

    def create_iter(self):
        #int(params.get("subs-tot al", "1000"))
        subs_per_part=self.subs_total/self.total_partitions
        i=0
        body=""

        #self._num_subs = int(params.get("subs-per-book", "100"))
        #hex(random.randint(0, total))
        for sub_id in range(int(subs_per_part*self.partition_index), int(subs_per_part*(self.partition_index+1))):
            body+=(json.dumps({ "update" : {"_id" : hex(sub_id), "_type" : self._index_name, "_index" : self._type_name} })+'\n')
            body+=(json.dumps({ "scripted_upsert":True, 
                               "script" : { "source": 
                                            "int [] bks=new int[params.count];"+
                                            "int n=0;"+
                                            "for(int i=0; i<params.count/5; ++i){bks[n]=(n*params.a+params.b)%params.max;++n;}"+
                                            "for(int i=0; i<params.count/5; ++i){bks[n]=(n*params.a+params.b)%params.max;++n;}"+
                                            "for(int i=0; i<params.count/5; ++i){bks[n]=(n*params.a+params.b)%params.max;++n;}"+
                                            "for(int i=0; i<params.count/5; ++i){bks[n]=(n*params.a+params.b)%params.max;++n;}"+
                                            "for(int i=0; i<params.count/5; ++i){bks[n]=(n*params.a+params.b)%params.max;++n;}"+
                                            "ctx._source.books=bks;", 
                                            "lang" : "painless", 
                                            "params" : {
                                                "count" : self.books_per_subs, 
                                                "a":primes[sub_id%len(primes)],
                                                "b":primes[(sub_id+13)%len(primes)], 
                                                "max":self.books_total}}, 
                               "upsert":{}})+'\n')
            #d = { }
            #books=[]
            #for s in range(0, self.books_per_subs):
            #    books.append(random.randint(0, self.books_total))
            #d["books"]=books
            #body+=(json.dumps(d)+'\n')
            i+=1
            if i%self.bulk_size==0:
                b=body
                yield {
                    "body":b,
                    "action-metadata-present":True,
                    "bulk-size":self.bulk_size,
                    "index":self._index_name,
                    "type":self._type_name
                }
                body=""
        if body!="":
            b=body
            yield {
                "body":b,
                "action-metadata-present":True,
                "bulk-size":self.bulk_size,
                "index":self._index_name,
                "type":self._type_name
            }
            body=""
        return
    
    def size(self):
        """"
        number of bulks per partition
        """
        return int(self.subs_total/self.total_partitions/self.bulk_size)
    
    def params(self):
        return next(self.iter)

def get_random_term_lookup_parameters(track, params, **kwargs):
    default_index = "books"
    default_type = "book"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    _max_subs = int(params.get("subs-total","1000"))
    return {
        "body":{
            "query": {
                "bool": {
                    "must": {
                        "match" : {"title": " ".join(random_text(2))}
                    },
                    "filter":{
                        "terms": {
                            "_id": {
                                "index": "subscriptions",
                                "type": "subscriptions",
                                "id": few_subscriptions(1,_max_subs)[0],
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
    
def get_random_subscriptions_update_query(track, params, **kwargs):
    default_index = "subscriptions"
    default_type = "subscriptions"
    index_name = params.get("index", default_index)
    type_name = params.get("type", default_type)
    body=""
    books_upd = int(self._factory._params.get("books-per-sub", "1000"))
    books_total = int(self._factory._params.get("books-total", "100000"))
    subs_total = int(self._factory._params.get("subs-total","2000"))
    books_nums=[]
    for x in range(0,books_upd):
        books_nums.append(random.randint(0, self.books_total))
    body+=(json.dumps({ "update" : {"_id" : few_subscriptions(1, subs_total)[0], "_type" : type_name, "_index" : index_name} })+'\n')
    body+=(json.dumps({"doc":{ "books" : books_nums, "updated":True}})+'\n')
    output = {
        "body":body,
        "action-metadata-present":True,
        "bulk-size":1,
        "index":index_name,
        "type":type_name
    }
    return output

def register(registry):
    registry.register_param_source("search-subs-filter", get_random_search_parameters)
    registry.register_param_source("search-subsDV-filter", get_random_searchDV_parameters)
    registry.register_param_source("update-subs-in-books", get_random_books_update_query)
    registry.register_param_source("insert-books-subs-parallel",InsertBooksSubsParamSource)
    registry.register_param_source("insert-books-only", InsertBooksOnly)
    registry.register_param_source("insert-subs-only", InsertSubsOnly)
    registry.register_param_source("search-terms-lookup", get_random_term_lookup_parameters)
    #registry.register_param_source("update-subs-only", get_random_subscriptions_update_query)
