# Assignment 2 Report: Simple Search Engine using Hadoop MapReduce

## Student Information

- Name: Dinar Yakupov
- Course: B23-DS-01

## Introduction

The goal of this assignment was to build a search engine using the tools from the course. The required technologies were Hadoop MapReduce for indexing, Cassandra or ScyllaDB for storage, and PySpark for ranking with BM25.

In my implementation, the workflow starts from one Parquet file, creates 1000 text documents, builds an inverted index with Hadoop Streaming, stores the final index in Cassandra, and runs search queries on YARN in distributed mode. The whole project follows the provided Docker template, so the main workflow can be started with `docker compose up`.

## Methodology

### Overall System Design

The project uses three main services defined in `docker-compose.yml`:

- `cluster-master`
- `cluster-slave-1`
- `cassandra-server`

The folder `app/` is mounted into the containers as `/app`. The main entrypoint is `app.sh`. This script starts Hadoop and YARN, creates the Python virtual environment, installs the required packages, prepares the data, builds the index, stores it in Cassandra, and then runs a sample query.

The main scripts are:

- `prepare_data.sh` and `prepare_data.py` for data preparation
- `create_index.sh` for Hadoop MapReduce indexing
- `store_index.sh` and `store_index.py` for loading the index into Cassandra
- `index.sh` to combine index creation and storage
- `search.sh` and `query.py` for searching

### Data Preparation

The dataset file used by the project is `a.parquet`. The expected local path is `/app/a.parquet`.

The script `prepare_data.sh` first checks that the Parquet file exists, uploads it to HDFS root, and then runs `prepare_data.py` with Spark. In `prepare_data.py`, I read the Parquet file with PySpark and select only the fields `id`, `title`, and `text`. After that, I remove rows with missing values and rows where the text is empty.

After filtering, the script checks that there are at least 1000 usable documents. If the dataset does not have enough valid rows, it stops. Otherwise, it keeps 1000 documents and continues.

### Document Format and Naming

Each document is written as a separate UTF-8 text file. The naming format is:

`<doc_id>_<doc_title>.txt`

This is implemented in `prepare_data.py`. The filename is sanitized using `sanitize_filename`, and spaces are replaced with underscores. This keeps the filenames valid while still preserving the document id and title.

The file content contains only the plain text of the article.

### Preparing `/data` and `/input/data`

After the local text files are created, they are uploaded to HDFS under `/data`.

Then the script reads the HDFS documents again with `wholeTextFiles()` and converts them into the required one-line format:

`<doc_id><tab><doc_title><tab><doc_text>`

These lines are written into `/input/data` in HDFS using `coalesce(1)`, so the result is a single partition as required in the assignment.

The main HDFS paths used in the project are:

- `/data`
- `/input/data`
- `/tmp/indexer`
- `/indexer`

### Index Construction with Hadoop MapReduce

The indexer is implemented with Hadoop Streaming in `create_index.sh`. The MapReduce scripts are stored in `app/mapreduce`.

This implementation uses two indexing pipelines.

#### Pipeline 1

The first pipeline reads `/input/data` and produces three kinds of records:

- document records
- vocabulary records
- index records

`mapper1.py` tokenizes the document text with a simple regular expression that keeps lowercase letters and digits. It computes:

- document length
- term frequency inside each document

For each input document, it emits:

- one `DOC` record with document id, title, and document length
- multiple `TERM` records with term, document id, title, term frequency, and document length

`reducer1.py` groups the term records by term. For each term, it calculates document frequency and emits:

- one `VOCAB` record for the vocabulary
- multiple `INDEX` records for postings
- the `DOC` records are also passed through unchanged

After pipeline 1, the output already contains the document list, postings, and vocabulary information needed for ranking.

#### Pipeline 2

The second pipeline computes corpus-level statistics for BM25.

`mapper2.py` reads the output of pipeline 1 and only keeps the `DOC` records. For each document, it emits a `CORPUS` record with the document length.

`reducer2.py` counts the number of documents and sums their lengths. From that, it calculates:

- total number of documents
- average document length

These values are later used by BM25.

### Intermediate and Final HDFS Outputs

Temporary output is written to `/tmp/indexer` during the Hadoop Streaming jobs.

After both pipelines finish, `create_index.sh` downloads the output files locally and passes them to `split_index_outputs.py`. This helper script separates the mixed pipeline output into four final files:

- `vocabulary.tsv`
- `index.tsv`
- `documents.tsv`
- `stats.tsv`

These files are then uploaded back to HDFS under `/indexer` as:

- `/indexer/vocabulary/part-00000`
- `/indexer/index/part-00000`
- `/indexer/documents/part-00000`
- `/indexer/stats/part-00000`

So the final index is stored in HDFS in separate folders, with one partition for each required component.

### Cassandra Schema Design

The script `store_index.py` connects to the Cassandra service `cassandra-server` and creates the keyspace `search_engine`.

It creates four tables:

- `vocabulary (term text PRIMARY KEY, df int)`
- `postings (term text, doc_id text, tf int, title text, doc_length int, PRIMARY KEY (term, doc_id))`
- `documents (doc_id text PRIMARY KEY, title text, doc_length int)`
- `corpus_stats (stat_name text PRIMARY KEY, stat_value double)`

This schema stores the main information needed for BM25 search:

- vocabulary with document frequency
- postings with term frequency and document length
- document metadata
- corpus statistics

The script reads the final index files from HDFS and inserts them into Cassandra one line at a time.

### Ranker Design and BM25 Calculation

The ranking application is implemented in `query.py`.

The query is tokenized with the same simple regex tokenizer used during indexing. Then for each query term, the program reads:

- the term document frequency from `vocabulary`
- the postings for that term from `postings`

For every posting, it computes a BM25 score. The implementation uses:

- term frequency `tf`
- document frequency `df`
- document length
- total number of documents
- average document length

The constants used are:

- `K1 = 1.0`
- `B = 0.75`

The score is calculated for each term-document pair, then the partial scores for the same document are added together. This aggregation is done with the PySpark RDD API using `parallelize`, `map`, `reduceByKey`, and `takeOrdered`. The final output is the top 10 ranked documents.

The output format is simple:

`doc_id<TAB>title`

### How `search.sh` Runs the Query on YARN

The script `search.sh` runs `query.py` with:

- `--master yarn`
- `--deploy-mode cluster`

This runs the query application on YARN in distributed mode. The script also sets the Python path so that the packages installed in `/app/.venv` are available to Spark.

The script uses explicit resource settings for this cluster:

- 1 executor
- 1 core
- 512 MB executor memory
- 512 MB driver memory

The query text is passed as a command-line argument, which fits the way the project runs in YARN cluster mode.

### Important Design Choices

The tokenizer lowercases text and keeps only letters and digits. I used the same rule in both indexing and querying so the terms are handled consistently.

Each Hadoop Streaming job uses one reducer. In this project, that produces one final output partition for each stage, which fits the later HDFS and Cassandra loading steps.

The Cassandra schema stores the vocabulary, postings, document metadata, and corpus statistics in separate tables. This matches what `query.py` needs during BM25 ranking.

The Docker cluster contains one master node, one worker node, and one Cassandra node. This setup is enough to demonstrate the required distributed workflow for the assignment.

## Demonstration

### How to Run the Project

From the repository root, the whole system can be started with:

```bash
docker compose up
```

Before running this command, `a.parquet` should be placed in:

```bash
app/a.parquet
```

The main automated flow is:

1. `docker compose up`
2. `cluster-master` runs `/app/app.sh`
3. `app.sh` starts Hadoop and YARN
4. a Python virtual environment is created
5. dependencies are installed from `requirements.txt`
6. `prepare_data.sh` runs
7. `index.sh` runs
8. `search.sh` runs a sample query

### What Happens During Each Stage

During service startup, `start-services.sh` starts HDFS, YARN, and the MapReduce history server. It also creates the Hadoop workers file so that only the existing nodes are used: `cluster-master` and `cluster-slave-1`.

During data preparation, `prepare_data.py` reads `a.parquet`, filters the data, creates 1000 UTF-8 text documents, uploads them to `/data`, and creates the single-partition file in `/input/data`.

During indexing, `create_index.sh` runs the two Hadoop Streaming jobs. After that, it splits the results into vocabulary, postings, documents, and corpus stats, and uploads them to `/indexer`.

During storage, `store_index.py` creates the Cassandra keyspace and tables, then inserts the data from HDFS into Cassandra.

During search, `search.sh` submits `query.py` to YARN in cluster mode. The application reads the index from Cassandra, calculates BM25 scores, and prints the top 10 documents.

### Commands Used for Manual Validation

After the system was running, I also used manual checks like these:

```bash
hdfs dfs -ls /data
hdfs dfs -ls /input/data
hdfs dfs -ls /indexer
bash search.sh "computer science"
bash search.sh "history"
```

These checks confirmed that the HDFS paths existed and that the search runner returned results.

### Example Query Results

I tested the system with the query `computer science`. Some of the returned titles were:

- `A B M Shawkat Ali`
- `A Brighter Summer Day (album)`
- `A Bug's Life`

I also tested the query `history`. The results for this query looked more obviously relevant. Some returned titles were:

- `A Briefer History of Time`
- `A Brief History of Chinese Fiction`
- `A Brief History of Everyone Who Ever Lived`

The system also stored corpus statistics in Cassandra successfully. In my successful run, the values included:

- `total_docs = 1000`
- `avg_doc_length = 575.759`

### Screenshot Placeholders

![Fullscreen screenshot of successful indexing](screenshots/indexing-success.png)

This screenshot should show the terminal after successful data preparation and indexing, including HDFS paths such as `/data`, `/input/data`, and `/indexer`.

![Fullscreen screenshot of search query 1](screenshots/query1.png)

This screenshot should show a successful search run for a query like `computer science` and the top returned document ids and titles.

![Fullscreen screenshot of search query 2](screenshots/query2.png)

This screenshot should show a successful search run for a second query like `history`, again with the top returned results visible in fullscreen.

## Results and Reflection

Overall, the search engine worked as expected. The full flow from Parquet input to HDFS, Hadoop MapReduce indexing, Cassandra storage, and YARN-based search ran successfully.

The `history` query gave more obviously relevant results than `computer science`. The ranking still worked for both, but the result quality depends a lot on the dataset and the normalization used in the project.

## Conclusion

In this assignment, I built a search engine pipeline that combines several big data tools. Hadoop MapReduce was used to build the index, Cassandra was used to store the final search data, and PySpark with BM25 was used to rank query results.

This project helped me understand how these components work together in practice. The most useful part for me was seeing the whole workflow, from raw dataset preparation to distributed search execution on YARN.

## References

- Assignment page: https://firas-jolha.github.io/bigdata/html/bs/BS%20-%20Assignment%202%20-%20Simple%20Search%20Engine%20using%20Hadoop%20MapReduce.html
- Apache Spark Documentation: https://spark.apache.org/docs/latest/
- Apache Cassandra Documentation: https://cassandra.apache.org/doc/latest/
