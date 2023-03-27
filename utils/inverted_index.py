# Import necessary packages
import json

from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, Normalizer, Word2Vec
from pyspark.sql.functions import (
    col,
    collect_list,
    concat_ws,
    explode,
    map_from_entries,
    size,
    struct,
    udf,
)
from pyspark.sql.types import (
    ArrayType,
    StringType,
)
import nltk
from nltk.stem import PorterStemmer

nltk.download('stopwords')


def stemming(words):
    return [ps.stem(w) for w in words]

ps = PorterStemmer()

stemming_udf = udf(stemming, ArrayType(elementType=StringType()))

# Create a Spark session
ss = SparkSession.builder.appName("SearchEngine").getOrCreate()

# Replace with your MongoDB connection string
mongo_uri = "your_mongodb_connection_string"
mongo_db = "your_database_name"
mongo_collection = "your_collection_name"

# Read data from MongoDB
raw = (
    ss.read.format("com.mongodb.spark.sql.DefaultSource")
    .option("spark.mongodb.input.uri", f"{mongo_uri}/{mongo_db}.{mongo_collection}")
    .load()
)

# Tokenize and remove stop words
tokenizer = RegexTokenizer().setPattern("\\W+").setInputCol("name").setOutputCol("words")
stop_words = StopWordsRemover.loadDefaultStopWords("english")
remover = StopWordsRemover(
    inputCol="words", outputCol="filtered_words", stopWords=stop_words
)

# Preprocess data: tokenize, remove stop words, and apply stemming
preprocessed_data = (
    tokenizer.transform(raw)
    .select("_id", "words")
    .select("_id", remover.transform(col("words")).alias("filtered_words"))
    .withColumn("tokens", stemming_udf("filtered_words"))
)

# Create inverted index
word_count_df = (
    preprocessed_data.select("_id", explode("tokens").alias("word"))
    .groupBy("word", "_id")
    .count()
    .groupBy("word")
    .agg(
        map_from_entries(collect_list(struct(concat_ws("", col("_id").cast(StringType())).alias("id"), "count"))).alias(
            "documents"
        )
    )
    .withColumnRenamed("word", "_id")
)

inverted_index = word_count_df.rdd.map(lambda row: (row["_id"], row["documents"])).collectAsMap()

# Compute document length
doc_length = (
    preprocessed_data.select("_id", size("tokens").alias("doc_length"))
    .withColumn("_id", concat_ws("", col("_id").cast(StringType())))
    .rdd.map(lambda row: (row["_id"], row["doc_length"]))
    .collectAsMap()
)

# Compute unique word count per document
unique_word = (
    preprocessed_data.select("_id", size("tokens").alias("num_unique_words"))
    .withColumn("_id", concat_ws("", col("_id").cast(StringType())))
    .rdd.map(lambda row: (row["_id"], row["num_unique_words"]))
    .collectAsMap()
)

product_metadata = raw.select("_id", "name").rdd.collectAsMap()

# Save the results to JSON files
with open("../data/inverted_index.json", "w") as f:
    json.dump(inverted_index, f)

with open("../data/doc_length.json", "w") as f:
    json.dump(doc_length, f)

with open("../data/unique_word.json", "w") as f:
    json.dump(unique_word, f)

with open("../data/product_metadata.json", "w") as f:
    json.dump(product_metadata, f)
