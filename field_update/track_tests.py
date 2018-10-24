import unittest
import json
from track import InsertBooksSubsParamSource
from track import get_random_search_parameters
from track import get_random_books_update_query

class TestBooksSubsInsert(unittest.TestCase):

    
    
    def test_insert(self):
        bulk_size=4
        c = InsertBooksSubsParamSource({"a":1},
                                       {
                        "bulk-size":bulk_size,
                        "subs-total":20,
                        "subs-per-book":2,
                        "books-total": 100 
                                       }
                                       )
        for p in range(0,3):
          part = c.partition(p, 3)
          bulks = part.size()
          for b in range(0, bulks):
              params = part.params()
              body=params["body"].split("\n")
              assert len(body)==bulk_size*2+1
              if p==0 and b==0:
                   print(params)
        return
    
    def test_query(self):
        bulk_size=4
        get_random_books_update_query({"a":1},
                                       {
                        "bulk-size":bulk_size,
                        "subs-total":20,
                        "subs-per-book":2,
                        "books-total": 100 
                                       })
        
    def test_query(self):
        bulk_size=4
        get_random_search_parameters({"a":1},
                                       {
                        "bulk-size":bulk_size,
                        "subs-total":20,
                        "subs-per-book":2,
                        "books-total": 100 
                                       })

if __name__ == '__main__':
    unittest.main()