from pyspark import SparkContext, rdd
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.functions import *
from nltk.corpus import stopwords
from nltk.stem.snowball import ItalianStemmer
from nltk import PorterStemmer
from nltk import FreqDist
import re
from preprocessing import item_preprocessing
from search_engine import cosine_sim

# Create the Spark Session context
spark = SparkSession.builder.appName("DataScraping").getOrCreate()
sc = SparkContext.getOrCreate()

# Read the Amazon item data
df = spark.read.option('delimiter', '\t').csv('items_data.tsv', header=True)

# Insert items id
df = df.withColumn("id", monotonically_increasing_id())

# Init the tokenizer
tokenizerR = RegexTokenizer(inputCol='description', outputCol='tokenized', pattern='\\W')

# Tokenize the item description text
df = tokenizerR.transform(df)

# Create the italian stopwords collection
stop_words = list(stopwords.words('italian'))

# Remove the italian stopwords from the item description
remover = StopWordsRemover(inputCol="tokenized", outputCol="stopwords_removed", stopWords=stop_words)
df = remover.transform(df)

# Last preprocess operations
ita_stemmer = ItalianStemmer()
eng_stemmer = PorterStemmer()

# Make the last preprocessing operations
def text_preprocessing(tokens):
    # Remove tokens composed by only a number
    filtered_token = [token for token in tokens if not re.search(r'\b[0-9]+\b\s*', token)]

    # Stem the tokens for both italian and english
    filtered_token = [ita_stemmer.stem(token) for token in filtered_token]
    filtered_token = [eng_stemmer.stem(token) for token in filtered_token]

    # Compute the term frequency
    filtered_token = FreqDist(filtered_token).most_common(50)

    return filtered_token

schema = ArrayType(StructType([
    StructField("char", StringType(), False),
    StructField("count", IntegerType(), False)
]))

col_text_preprocessing = udf(lambda x: text_preprocessing(x), schema)
spark.udf.register('col_text_preprocessing', col_text_preprocessing)

df = df.withColumn('preprocessed_descr', col_text_preprocessing('stopwords_removed'))

# Build the inverted index
inverted_index = df.select('id', 'preprocessed_descr')\
    .rdd.flatMap(lambda row: [(row[0], token) for token in row[1]])\
    .map(lambda row: (row[1][0], [(row[0], row[1][1])]))\
    .reduceByKey(lambda a,b: a+b)

inverted_index = inverted_index.toDF(['term', 'docs'])

# Register the computing cosine similarity function
def col_cos_sim(preprocessed_q):
    return udf(lambda x: cosine_sim(preprocessed_q, x))

spark.udf.register('col_cos_sim', col_cos_sim)

# Build the search engine system
def spark_search_engine(query, inverted_index, data):
    """
    Building the search engine where the results are orederd by cosine similarity
    :param query:
    :param inverted_index:
    :param data:
    :return:
    """

    # Preprocessing the query text
    preprocessed_q = item_preprocessing(query)

    # Filter the inverted index by selecting only the terms contained in the query
    term_list = []
    for (term, freq) in preprocessed_q:
        term_list.append(term)

    rows = inverted_index.filter(inverted_index.term.isin(term_list))
    docs = rows.select(rows.docs).collect()

    # Collecting the docs IDs that contained at least one term of the query
    ids = []
    for row in docs:
        for (doc_id, freq) in row.docs:
            ids.append(doc_id)

    data = data.filter(data.id.isin(ids))

    # Computing the cosine similarity among the remaining docs
    results = data.withColumn('cos_sim', col_cos_sim(preprocessed_q)(data.preprocessed_descr))

    # Order the results by the cosine similarity
    results = results.orderBy('cos_sim', ascending=False)

    return results

# Set the query string
QUERY_STRING = 'portatile con i7 e 16gb di ram'

# Start the search engine
result = spark_search_engine(QUERY_STRING, inverted_index, df)

# Show the query results
result.show(20, truncate=False)

# Close the spark context
spark.stop()
