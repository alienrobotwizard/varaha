--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
-- 
--     http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

register '../../target/varaha-1.0-SNAPSHOT.jar';
register '../../lib/lucene-core-3.1.0.jar';

-- 
-- Load and tokenize the raw documents
-- 
raw_documents = LOAD '$DOCS' AS (doc_id:chararray, text:chararray);
tokenized     = FOREACH raw_documents GENERATE doc_id AS doc_id, FLATTEN(varaha.text.TokenizeText(text)) AS (token:chararray);
   
-- 
-- Count the number of times each (doc_id,token) pair occurs. (term counts)
-- 
doc_tokens       = GROUP tokenized BY (doc_id, token);
doc_token_counts = FOREACH doc_tokens GENERATE FLATTEN(group) AS (doc_id, token), COUNT(tokenized) AS num_doc_tok_usages;

--
-- Attach the document size to each record
--
doc_usage_bag    = GROUP doc_token_counts BY doc_id;
doc_usage_bag_fg = FOREACH doc_usage_bag GENERATE
                     group                                                 AS doc_id,
                     FLATTEN(doc_token_counts.(token, num_doc_tok_usages)) AS (token, num_doc_tok_usages), 
                     SUM(doc_token_counts.num_doc_tok_usages)              AS doc_size
                   ;

--
-- Next, generate the term frequencies
--
term_freqs = FOREACH doc_usage_bag_fg GENERATE
               doc_id                                          AS doc_id,
               token                                           AS token,
               ((double)num_doc_tok_usages / (double)doc_size) AS term_freq;
             ;
             
--
-- Then, find the number of documents that contain at least one occurrence of term
--
term_usage_bag  = GROUP term_freqs BY token;
token_usages    = FOREACH term_usage_bag GENERATE
                    FLATTEN(term_freqs) AS (doc_id, token, term_freq),
                    COUNT(term_freqs)   AS num_docs_with_token
                   ;

--
-- Generate the tf-idf for each (doc_id, token, pair)
--
tfidf_all = FOREACH token_usages {
              idf    = LOG((double)$NDOCS/(double)num_docs_with_token);
              tf_idf = (double)term_freq*idf;
                GENERATE
                  doc_id AS doc_id,
                  token  AS token,
                  tf_idf AS tf_idf
                ;
             };

--
-- Finally generate term vectors for later processing
--
grouped = GROUP tfidf_all BY doc_id;
vectors = FOREACH grouped GENERATE group AS doc_id, tfidf_all.(token, tf_idf) AS vector;

STORE vectors INTO '$TFIDF';
