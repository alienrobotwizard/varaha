register ../../lib/stanford-postagger-withModel.jar 
register ../../target/varaha-1.0-SNAPSHOT.jar

rmf /tmp/tagged.txt

reviews = LOAD '/tmp/reviews.avro' USING AvroStorage();
reviews = LIMIT reviews 1000;
foo = FOREACH reviews GENERATE business_id, varaha.text.StanfordPOSTagger(text) as tagged;
STORE foo INTO '/tmp/tagged.txt';
