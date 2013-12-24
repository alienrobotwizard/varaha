register ../../lib/stanford-postagger-withModel.jar 
register ../../target/varaha-1.0-SNAPSHOT.jar

rmf /tmp/tagged.txt

reviews = LOAD '/tmp/reviews.avro' USING AvroStorage();
reviews = LIMIT reviews 1000;
foo = FOREACH reviews GENERATE business_id, varaha.text.StanfordTokenize(text) AS tagged;
DUMP foo

reviews = LOAD '/tmp/reviews.avro' USING AvroStorage();
reviews = LIMIT reviews 1000;
bar = FOREACH reviews GENERATE business_id, FLATTEN(varaha.text.SentenceTokenize(text)) AS tokenized_sentences;
bar = FOREACH bar GENERATE business_id, varaha.text.StanfordPOSTagger(tokenized_sentences) AS tagged;
DUMP bar

reviews = LOAD '/tmp/reviews.avro' USING AvroStorage();
reviews = LIMIT reviews 1000;
bar = FOREACH reviews GENERATE business_id, varaha.text.StanfordPOSTagger(varaha.text.StanfordTokenize(text)) AS tokens;
DUMP bar