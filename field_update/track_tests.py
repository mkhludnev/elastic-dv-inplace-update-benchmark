import unittest
from track import InsertBooksSubsParamSource

class TestBooksSubsInsert(unittest.TestCase):

    def test_math(self):
        c = InsertBooksSubsParamSource({"a":1},
                                       {
                        "bulk-size":1000,
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
              if p==0 and b==0:
                   print(params)
        return

if __name__ == '__main__':
    unittest.main()