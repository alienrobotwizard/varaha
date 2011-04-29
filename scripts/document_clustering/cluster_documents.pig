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

vectors   = LOAD '$TFIDF'        AS (doc_id:chararray, vector:bag {t:tuple (token:chararray, weight:double)});
k_centers = LOAD '$CURR_CENTERS' AS (doc_id:chararray, vector:bag {t:tuple (token:chararray, weight:double)});


--
-- Compute the similarity between all document vectors and each of the K centers
--

--
-- FIXME: this can be optimized for K large, cross is dangerous
--
with_centers = CROSS k_centers, vectors;
similarities = FOREACH with_centers GENERATE
                 k_centers::doc_id AS center_id,
                 k_centers::vector AS center,
                 vectors::doc_id   AS doc_id,
                 vectors::vector   AS vector,
                 varaha.text.TermVectorSimilarity(k_centers::vector, vectors::vector) AS similarity
               ;

--
-- Foreach vector, find the nearest center
--
finding_nearest = GROUP similarities BY doc_id;
only_nearest    = FOREACH finding_nearest {
                    nearest_center = TOP(1, 4, similarities);
                    GENERATE
                      FLATTEN(nearest_center) AS (center_id, center, doc_id, vector, similarity)
                    ;
                  };

--
-- Get the number of vectors associated with the center
--
center_grpd = GROUP only_nearest BY center_id;
center_cnts = FOREACH center_grpd GENERATE FLATTEN(only_nearest) AS (center_id, center, doc_id, vector, similarity), COUNT(only_nearest) AS num_vectors;

--
-- Calculate the centroids in a distributed fashion
--
cut_nearest    = FOREACH center_cnts GENERATE center_id AS center_id, num_vectors AS num_vectors, FLATTEN(vector) AS (token:chararray, weight:double);
centroid_start = GROUP cut_nearest BY (center_id, token);
weight_avgs    = FOREACH centroid_start GENERATE FLATTEN(group) AS (center_id, token), (double)SUM(cut_nearest.weight)/(double)MAX(cut_nearest.num_vectors) AS weight_avg;
centroids_grp  = GROUP weight_avgs BY center_id;

--
-- Need to keep from having humungous vectors
--
centroids      = FOREACH centroids_grp {
                   -- 
                   -- FIXME: for some reason TOP($MAX_CENTER_SIZE, 2, weight_avgs) throws NPE
                   --
                   ordrd            = ORDER weight_avgs BY weight_avg DESC;  
                   shortened_vector = LIMIT ordrd $MAX_CENTER_SIZE;
                   GENERATE
                     group            AS center_id,
                     shortened_vector.($1, $2) AS vector
                   ;
                 };

STORE centroids INTO '$NEXT_CENTERS';
