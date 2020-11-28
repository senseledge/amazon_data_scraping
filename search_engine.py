import collections, math
import pandas as pd
from preprocessing import item_preprocessing

def build_inverted_index(docs):
    '''
    Building an inverted index from my items description collection
    :param docs:
    :return:
    '''

    term_list = []

    # Init an empty inverted index
    inverted_index = collections.defaultdict(list)

    # Scanning all the docs and mapping term, docs id and frequency of the given term in the doc
    for i in range(len(docs)):
        for (term, freq) in docs.iloc[i].preprocessed_descr:
            term_list.append((term, (docs.iloc[i].id, freq)))

    # Reduce the maps in key (term): value (list((docID, freq),...)) in order to build an inverted index
    for (term, id_freq) in term_list:
        inverted_index[term].append(id_freq)

    # Converting the inverted index from a dict to a Pandas' Serie
    inverted_serie = pd.Series(inverted_index).rename_axis('terms').reset_index()

    # Converting the inverted index from a Serie to a DataFrame
    df = pd.DataFrame(inverted_serie)
    df.columns = ['terms', 'docs']

    return df

def cosine_sim(q,d):
    """
    Computing the cosine similarity between two docs intended as 'Query Text' and 'Doc Text'
    :param q:
    :param d:
    :return:
    """

    # Init valuse
    q_sum = 0
    d_sum = 0
    num = 0

    # Computing the norm of d and q
    for (term, freq) in q:
        q_sum += freq**2

    for (term, freq) in d:
        d_sum += freq**2

    den = math.sqrt(q_sum) * math.sqrt(d_sum)

    # Computing the product of each component of the two vectors
    for (q_term, q_freq) in q:
        for (d_term, d_freq) in d:
            if q_term == d_term:
                i = q_freq * d_freq
                num += i

    if den != 0:
        return num/den
    else:
        return 0

def search_engine(query, inverted_index, data):
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

    index_mask = inverted_index['terms'].isin(term_list)
    rows = inverted_index.loc[index_mask]

    # Collecting the docs IDs that contained at least one term of the query
    ids = []
    for i in range(0, len(rows)):
        for (doc_id, freq) in rows.iloc[i].docs:
            ids.append(doc_id)

    data_mask = data['id'].isin(ids)
    results = data.loc[data_mask]

    # Computing the cosine similarity among the remaining docs
    results['cos_sim'] = results['preprocessed_descr'].apply(lambda x: cosine_sim(preprocessed_q, x))

    # Order the results by the cosine similarity
    results = results.sort_values(by=['cos_sim'], ascending=False)

    return results