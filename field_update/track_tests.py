import unittest
import json
import jinja2
import io
import track 

class TestBooksSubsInsert(unittest.TestCase):

    def test_insert_books_and_subs(self):
        bulk_size=4
        c = track.InsertBooksSubsParamSource({"a":1},
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
              #notice tailing \n
              for i in range(0,len(body)-1,2):
                  #assert body[i]
                  docs = body[i+1]
                  doc = json.loads(docs)
                  assert "subscriptions" in doc
                  assert len(doc["subscriptions"])==2
              if p==0 and b==0:
                   print(params)
        return

    def test_insert_books_no_subs(self):
        bulk_size=3
        c = track.InsertBooksOnly({"a":1},
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
              #notice tailing \n
              for i in range(0,len(body)-1,2):
                  docs = body[i+1]
                  doc = json.loads(docs)
                  assert not "subscriptions" in doc
              if p==0 and b==0:
                   print(params)
        return

    def test_insert_subs(self):
        bulk_size=4
        c = track.InsertSubsOnly({"a":1},
                                       {
                        "bulk-size":bulk_size,
                        "subs-total":20,
                        "books-per-sub":200,
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
              #notice tailing \n
              for i in range(0,len(body)-1,2):
                  meta = json.loads(body[i])
                  assert "index" in meta
                  id = int(meta["index"]["_id"],16)
                  assert id>=0 and id<20, "%s"%id
                  docs = body[i+1]
                  doc = json.loads(docs)
                  bookslist = doc["books"]
                  assert len(bookslist)==200, "%d =>%s" % (id,bookslist)
                  for b in bookslist:
                      assert b>=0 and b<=100, "%d"%b
              if p==0 and b==0:
                   print(params)
        return
    
        
    def test_update_subs_in_books(self):
        bulk_size=4
        track.get_random_books_update_query({"a":1},
                                       {
                        "bulk-size":bulk_size,
                        "subs-total":20,
                        "subs-per-book":2,
                        "books-total": 100 
                                       })
        
    def test_query_books_by_subs(self):
        bulk_size=4
        track.get_random_search_parameters({"a":1},
                                       {
                        "bulk-size":bulk_size,
                        "subs-total":20,
                        "subs-per-book":2,
                        "books-total": 100 
                                       })
    def test_json(self):
        env = jinja2.Environment(loader=jinja2.FileSystemLoader("."))
        #template_name=io.basename(template_file_name)
        tmp=env.get_template("track.json")
        j=tmp.render()
        #print(j)
        json.loads(j)
        
if __name__ == '__main__':
    unittest.main()