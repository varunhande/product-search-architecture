import json
import math
import time
import re
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

# Term Frequency
def tf(word_occ, word_count):
    return word_occ / word_count

# Inverse Document Frequency
def idf(total_doc_count, doc_count_cont_word):
    return math.log(total_doc_count / doc_count_cont_word)

# BM25
def bm25(word_occurrences, word_count, document_count, documents_containing_word, all_document_word_count, b=0.75, k=1.2):
    _idf = idf(document_count, documents_containing_word)
    _tf = tf(word_occurrences, word_count)
    _avg_dl = all_document_word_count / document_count

    score = _idf * (_tf * (k + 1)) / (_tf + k * (1 - b + b * word_count / _avg_dl))
    return score

# Preprocess the text
def preprocess(text):
    words = re.split(pattern, text.lower())
    tokens = [w for w in words if w]
    stop_words = set(stopwords.words("english"))
    tokens = [w for w in tokens if w not in stop_words]

    res = []
    for w in words:
        res.append(ps.stem(w))
    return res

def get_bm25_results(query, top_n):
    text = preprocess(query)
    topic_words = {}

    for word in text:
        topic_words[word] = topic_words.get(word, 0) + 1

    n = len(text)

    results = score(n, topic_words)
    results_ranked = dict(sorted(results.items(), key=lambda item: item[1], reverse=True))
    products = [product_metadata[int(id)] for id in list(results_ranked.keys())[:top_n]]
    
    return products

def score(n, topic_words):
    scores = {}
    for word, occurrence in topic_words.items():
        word_score = bm25(
            occurrence,
            n,
            doc_length["doc_counter"],
            len(inverted_index.get(word, [])) + 1,
            doc_length["word_counter"],
        )
        for doc_id, doc_occurrence in inverted_index.get(word, {}).items():
            doc_score = bm25(
                doc_occurrence,
                doc_length[doc_id],
                doc_length["doc_counter"],
                len(inverted_index.get(word, [])) + 1,
                doc_length["word_counter"],
            )
            if scores.get(doc_id) == None:
                scores[doc_id] = word_score * doc_score
            else:
                scores[doc_id] = scores[doc_id] + word_score * doc_score
    return scores

def get_search_results(query, model_type, n=10):
    start_time = time.time()
    if model_type == 'bm25':
        results = get_bm25_results(query, n)
    else:
        print("Invalid model type")
        return

    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds\n")
    print(f"Your search query: {query}")
    for i, pd in enumerate(results):
        print(f"{i}. {pd}")

with open("../data/inverted_index.json", "r") as f:
    inverted_index = json.load(f)

with open("../data/doc_length.json", "r") as f:
    doc_length = json.load(f)

with open("../data/unique_word.json", "r") as f:
    unique_word = json.load(f)

with open("../data/product_metadata.json", "r") as f:
    product_metadata = json.load(f)

# Define a regex pattern to split text into words
pattern = r"[^a-zA-Z0-9]+"

# Initialize PorterStemmer
ps = PorterStemmer()

# Sample queries to test the search
query = "sneakers"
get_search_results(query, "bm25")

query = "t-shirt"
get_search_results(query, "bm25")
