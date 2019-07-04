This is the unfinished set of Elasticsearch rally tracks that should represent performance on updating simple book metadata document.
At this moment the set has two folders, each of which contains one corresponding rally track.
To run these tracks you should have elasticsearch rally installed on your computer. 
Please, see https://github.com/elastic/rally for detailed instructions on rally installation.

The benchmark reads words from /usr/share/dict/words

 export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
 
 sudo add-apt-repository ppa:openjdk-r/ppa \
&& sudo apt-get update -q \
&& sudo apt install -y openjdk-11-jdk

$esrally --distribution-version=6.4.0 --track-path=track.json --offline --preserve-install true --car="8gheap"  --client-options="timeout:6000" &

  --revsion current
  --report-file REPORT_FILE
                        Write the command line report also to the provided file.
  --quiet   
  
  
  sudo add-apt-repository ppa:linuxuprising/java
sudo apt update
sudo apt install oracle-java12-installer


 export JAVA_HOME=/usr/lib/jvm/java-12-oracle
 export JAVA11_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
  
 :/mnt/tb/src/elasticsearch$ patch -p 1 -i ~/dv.patch
 
 esrally --revision current --offline --track-path=track.json --preserve-install true --car="8gheap"  --client-options="timeout:6000" --report-file ../rally.out  --quiet &
 
 $ curl 'localhost:39200/books/_search?size=1' -H Content-Type:application/json -d '{"query":{"bool":{"filter":{"term":{"subscription_0x75d":1}}}}}'
{"took":62,"timed_out":false,"_shards":{"total":100,"successful":100,"skipped":0,"failed":0},"hits":{"total":{"value":10000,"relation":"gte"},"max_score":0.0,"hits":[{"_index":"books","_type":"books","_id":"12306296","_score":0.0,"_source":{"title": "underplays parterre's prolongations O'Casey", "author": "specializing insatiable", "abstract": "internationalism's Cantabrigian parachute shag's Guallatiri triplicated potbelly Edsel's inconspicuousness's Penny's tardily trills sixpence repulsion's midday's tuna singles Angkor's don's flatulence's Babar pullouts devotion's ogles sacks disgruntle Gaia ocelot's stemmed interpretations Tulsidas masterstroke Potts market bronze bewildered Camel spadefuls Shiloh's Skye's nowhere oaring fulls tanager's nationalism Kelley's Liszt unevenness picturing Glasgow promiscuity's giants ruder wastage scarcer headlined newtons insurrectionists spitefuller Wrigley's Cotonou biodiversity's acted Lyman's realm snapper disenchants Levant's rationalizations carcinomata rued succeed reinvest killdeer civility's ironing's salamander's retrieval's sliding Mecca's relearn exorcist mutinously scalawag imbuing buses rugrats nonevent's contractions rowdyism deceiving trapezes enthusiasm's Wilmer reclusive scapegoat's ingratitude's further reconnoiter spoke's", "pubDate": 1994, "subscriptions": ["0x306", "0x75d", "0x76f", "0x1c7", "0x48a"], "subscription_0x306": 1, "subscription_0x75d": 1, "subscription_0x76f": 1, "subscription_0x1c7": 1, "subscription_0x48a": 1}}]}}


$ curl 'localhost:39200/books/books/12306296/_explain?' -H Content-Type:application/json -d '{"query":{"bool":{"filter":{"term":{"subscription_0x75d":1}}}}}'
{"_index":"books","_type":"books","_id":"12306296","matched":true,"explanation":{"value":0.0,"description":"ConstantScore(subscription_0x75d:[1 TO 1])^0.0","details":[]}}



0$ curl 'localhost:39200/books/books/_validate/query?explain=true' -H Content-Type:application/json -d '{"query":{"bool":{"filter":{"term":{"subscription_0x75d":0}}}}}'
{"_shards":{"total":1,"successful":1,"failed":0},"valid":true,"explanations":[{"index":"books","valid":true,"explanation":"+(#subscription_0x75d:[0 TO 0]) #*:*"}]}

$ curl 'localhost:39200/books/_mapping'
{"books":{
         "mappings":{
                "properties":{
                          "abstract":
                               {"type":"text",
                                "fields":{
                                       "keyword":{"type":"keyword","ignore_above":256}}},
                          "author":{"type":"text",
                                 "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                          "pubDate":{"type":"long"},
                          "subscription_0x0":{"type":"long"},
                          "subscription_0x1":{"type":"long"},
                          "subscription_0x10":{"type":"long"},
                          "subscription_0x100":{"type":"long"},"
                          subscription_0x101":{"type":"long"},


$ curl 'localhost:39200/books/_mapping/field/subscription_0x101?include_defaults=true'
{"books":{"mappings":{"subscription_0x101":{"full_name":"subscription_0x101","mapping":{"subscription_0x101":{"type":"long","boost":1.0,"index":true,"store":false,"doc_values":true,"term_vector":"no","norms":false,"index_options":"positions","eager_global_ordinals":false,"similarity":"BM25","ignore_malformed":false,"coerce":true,"null_value":null}}}}}}


