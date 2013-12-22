register ../../lib/stanford-postagger-withModel.jar 
register ../../target/varaha-1.0-SNAPSHOT.jar

rmf /tmp/tagged.txt

reviews = LOAD '/tmp/reviews.avro' USING AvroStorage();
reviews = LIMIT reviews 1000;
foo = FOREACH reviews GENERATE business_id, varaha.text.StanfordPOSTagger(text) AS tagged;
DUMP foo
-- STORE foo INTO '/tmp/tagged.txt';

reviews = LOAD '/tmp/reviews.avro' USING AvroStorage();
reviews = LIMIT reviews 1000;
bar = FOREACH reviews GENERATE business_id, varaha.text.StanfordPOSTagger(varaha.text.SentenceTokenize(text)) AS tokenized_sentences;
-- DUMP bar

reviews = LOAD '/tmp/reviews.avro' USING AvroStorage();
reviews = LIMIT reviews 1000;
bar = FOREACH reviews GENERATE business_id, varaha.text.StanfordTokenize(text) AS tokens;
-- DUMP bar