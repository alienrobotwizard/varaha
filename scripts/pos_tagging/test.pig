register ../../lib/stanford-postagger-withModel.jar 
register ../../target/varaha-1.0-SNAPSHOT.jar

reviews = LOAD 'data/ten.avro' USING AvroStorage;
foo = FOREACH reviews GENERATE business_id, varaha.text.StanfordTokenize(text) AS tokenized;
DUMP foo

reviews = LOAD 'data/ten.avro' USING AvroStorage();
reviews = LIMIT reviews 1000;
bar = FOREACH reviews GENERATE business_id, FLATTEN(varaha.text.SentenceTokenize(text)) AS tokenized_sentences;
bar = FOREACH bar GENERATE business_id, varaha.text.StanfordPOSTag(tokenized_sentences) AS tagged;
DUMP bar

reviews = LOAD 'data/ten.avro' USING AvroStorage();
reviews = LIMIT reviews 1000;
bar = FOREACH reviews GENERATE business_id, varaha.text.StanfordPOSTag(varaha.text.StanfordTokenize(text)) AS tokens;
DUMP bar