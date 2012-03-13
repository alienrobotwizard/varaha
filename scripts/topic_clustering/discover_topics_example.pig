register '../../target/varaha-1.0-SNAPSHOT.jar';
register '../../lib/mallet-2.0.7-RC2.jar';
register '../../lib/trove-2.0.4.jar';
register '../../lib/lucene-core-3.1.0.jar';
register '../../lib/pygmalion-1.1.0-SNAPSHOT.jar';

define TokenizeText varaha.text.TokenizeText();
define LDATopics varaha.topic.LDATopics();
define RangeConcat org.pygmalion.udf.RangeBasedStringConcat('0', ' ');

-- 
-- Load the docs
-- 
raw_documents = load '$DOCS' as (doc_id:chararray, text:chararray);

--
-- Tokenize text to remove stopwords
--
tokenized = foreach raw_documents generate doc_id AS doc_id, flatten(TokenizeText(text)) as (token:chararray);

--
-- Concat the text for a given doc with spaces
--
documents = foreach (group tokenized by doc_id) generate group as doc_id, RangeConcat(tokenized.token) as text;

--
-- Ensure all our documents are sane
--
for_lda = filter documents by SIZE(doc_id) > 0 and SIZE(text) > 0;

--
-- Group the docs by all and find topics
--
-- WARNING: This is, in general, not appropriate in a production environment.
-- Instead it is best to group by some piece of metadata which partitions
-- the documents into smaller groups.
--
topics = foreach (group for_lda all) generate
           FLATTEN(LDATopics(20, for_lda)) as (
           topic_num:int,
           keywords:bag {t:tuple(keyword:chararray, weight:int)}
         );


store topics into '$OUT';
