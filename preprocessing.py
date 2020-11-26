import pandas as pd
from nltk import pos_tag
from nltk.corpus import stopwords
#nltk.download('stopwords')
from nltk.tokenize import word_tokenize, RegexpTokenizer
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import sent_tokenize

def import_items(tsv_file):
    df = pd.read_csv(tsv_file, delimiter='\t')
    data = pd.DataFrame(df, columns=['description', 'price', 'prime', 'link', 'stars'])

    pd.options.display.max_columns = None
    pd.options.display.width = None

    # print(data.head(10))
    return data

def item_preprocessing(descr_text):
    tokenizer = RegexpTokenizer(r'\w+')
    token_list = tokenizer.tokenize(descr_text.lower())

    filtered_token = []
    stop_words = set(stopwords.words('italian'))
    for token in token_list:
        if token not in stop_words:
            filtered_token.append(token)

    return filtered_token

data = import_items('items_data.tsv')

data['preprocessed_descr'] = data['description'].apply(item_preprocessing)

print(data)
prova = list(data['preprocessed_descr'])
print(prova[0])
