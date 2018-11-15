This is the unfinished set of Elasticsearch rally tracks that should represent performance on updating simple book metadata document.
At this moment the set has two folders, each of which contains one corresponding rally track.
To run these tracks you should have elasticsearch rally installed on your computer. 
Please, see https://github.com/elastic/rally for detailed instructions on rally installation.

The benchmark reads words from /usr/share/dict/words

$esrally --distribution-version=6.4.0 --track-path=track.json --offline --preserve-install true --car="8gheap"  --client-options="timeout:6000" &


