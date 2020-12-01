# Data Mining - Homework 2

## Scarping script Usage
* Run the Python module scraping.py to collect Amazon products' data.

The data is composed by description, price, URL link, review star and prime (True = it's a Prime product) and it's persisted on the items_data.tsv.
The following search engine use this data to perform their query searchs

## Search Engine Usage (Pandas version)
* In the Python module main.py set the query by assign at the variable QUERY_STRING your string
* Run the module main.py

### Inverted index
inverted_index.csv is the builded inverted index at runtime

## Search Engine Usage (Spark version)
* In the Python module spark_main.py set the query by assign at the variable QUERY_STRING your string
* Run the module spark_main.py

### Inverted index
inverted_index is the Spark RDD of the builded inverted index

(In the 'screenshoots' directory there are short and long query test both for Pandas and Spark search engine)

## COVID ML Usage
* Download the train.csv into the covid_dataset directory
* Run the Python module main_covid.py
