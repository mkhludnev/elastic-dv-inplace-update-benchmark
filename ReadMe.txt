This is the unfinished set of Elasticsearch rally tracks that should represent performance on updating simple book metadata document.
At this moment the set has two folders, each of which contains one corresponding rally track.
To run these tracks you should have elasticsearch rally installed on your computer. Please, see https://github.com/elastic/rally for detailed instructions on rally installation.
To run a task, please open your terminal, navigate to required folder(field_update or index_update)  and type following command:

esrally --distribution-version=6.4.0 --track-path=./

