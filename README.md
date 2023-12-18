# SearchEngine
Project work for Multimedia Information Retrieval and Computer Vision course, MSc in Artificial Intelligence and Data Engineering @ UNIPI, A.Y. 2023/2024<br>
All the implementative details and project structure are explained in the documentation.<br>

Authors: 
- Domenico D'Orsi
- Denny Meini
- Matteo Razzai

## How to run the project
### Indexing
In order to perform the indexing operation, it is necessary to create inside the folder *"indexer"* a new one called *"data"*, then 
inside of it the preferred collection of documents can be inserted.
In this way, we avoid uploading a collection of several gigabytes in size, which would significantly weigh down the repository. Additionally, it allows the user to choose a collection according to their preference.<br>
The collection we referred to for the purpose of the project is the *MSMARCO passages* available at https://microsoft.github.io/msmarco/TREC-Deep-Learning-2020.<br><br>
After this, the main method of the class `indexer/src/main/java/it/unipi/mircv/IndexerMain.java` can be executed and will perform both 
SPIMI algorithm and merging: inside of the main method, by means of two flags passed as parameters to the respective methods, it can be specified if the document collection
is compressed or not and if the files produced as output by the merging algorithm are for debugging purpose (so they will be created in .txt format) or not (binary format).<br><br>
After the process is over, inside of the data folder previously created all the necessary files (inverted index, vocabulary
document index and blocks informations) are now present.<br>

### Performing queries and evaluating the system
After the indexing phase, the system is ready to receive queries. By executing the main method of the class `tester/src/main/java/it/unipi/mircv/Tester.java` the user
can interact with the system and perform queries, selecting between several options:<br>

- Number of results to be shown
- Conjunctive or disjunctive query
- DAAT or MaxScore algorithm (for disjunctive ones)
- TFIDF or BM25 scoring function

The query results will then be reported on the Command Line.<br>
If, instead, the user wants to perform an evaluation of the Search engine some more steps are required: once again, it is necessary to create inside the folder *"tester"* a new one called *"data"*, in which a qrels file has to be inserted in uncompressed format. The one we referred to for the purposes of the project is the *msmarco-test2020-queries.tsv* available at https://microsoft.github.io/msmarco/TREC-Deep-Learning-2020 <br><br>
After uploading the qrels file, the user can select the *evaluate system* option after running the Tester class main method, along with the preferred scoring function, number of results to be retrieved and algorithm to be used; 
the output of this operation is a file called *evaluation_results.txt*, which can be found inside an automatically created folder *evaluation* inside of the tester module.<br><br>
This file contains, for each query in the qrel file, the results retrieved by the search engine with the relative score obtained: this file can be submitted
to *trec eval* to verify the performance metrics obtained by the system.
