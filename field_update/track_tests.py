import unittest
import json
from track import InsertBooksSubsParamSource

class TestBooksSubsInsert(unittest.TestCase):

    def test_math(self):
        bulk_size=1000
        c = InsertBooksSubsParamSource({"a":1},
                                       {
                        "bulk-size":bulk_size,
                        "subs-total":2000,
                        "subs-per-book":10,
                        "books-total": 10000000 
                                       }
                                       )
        for p in range(0,10):
          part = c.partition(p, 10)
          bulks = part.size()
          for b in range(0, bulks):
              params = part.params()
              body=params["body"].split("\n")
              assert len(body)==bulk_size*2+1
              if p==0 and b==0:
                   print(params)
        return

if __name__ == '__main__':
    unittest.main()