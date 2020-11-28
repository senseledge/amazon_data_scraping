import pandas as pd
import re
from nltk import FreqDist
from nltk.stem.snowball import ItalianStemmer
from nltk import PorterStemmer
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer

def import_items(tsv_file):
    """
    Import dat from the tsv file and convert it in a Pandas dataframe
    :param tsv_file:
    :return:
    """
    df = pd.read_csv(tsv_file, delimiter='\t')
    data = pd.DataFrame(df, columns=['description', 'price', 'prime', 'link', 'stars'])
    data['id'] = range(len(data))

    # Setting dataframe pretty print
    pd.options.display.max_columns = None
    pd.options.display.width = None

    return data

def item_preprocessing(descr_text):
    """
    Preprocess of a text string
    :param descr_text:
    :return:
    """

    # Tokenizing the string by excluding all the special characters that are not alphanumeric and underscore
    tokenizer = RegexpTokenizer(r'\w+')
    token_list = tokenizer.tokenize(descr_text.lower())

    # Creating a italian step words dictionary
    stop_words = set(stopwords.words('italian'))

    # Creating both italian and english stemmers due to the particular dataset with mixed languages
    ita_stemmer = ItalianStemmer()
    eng_stemmer = PorterStemmer()

    # Removing stop words
    filtered_token = [token for token in token_list if not token in stop_words]

    # Removing tokens composed by only a number
    filtered_token = [token for token in filtered_token if not re.search(r'\b[0-9]+\b\s*', token)]

    # Stemming the tokens for both italian and english
    filtered_token = [ita_stemmer.stem(token) for token in filtered_token]
    filtered_token = [eng_stemmer.stem(token) for token in filtered_token]

    filtered_token = FreqDist(filtered_token).most_common(50)

    return filtered_token

data = import_items('items_data.tsv')

data['preprocessed_descr'] = data['description'].apply(item_preprocessing)


print(data)
