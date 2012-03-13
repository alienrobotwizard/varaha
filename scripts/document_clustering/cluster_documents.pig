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

vectors   = LOAD '$TFIDF'        AS (doc_id:chararray, norm_sq:double, vector:bag {t:tuple (token:chararray, weight:double)});
k_centers = LOAD '$CURR_CENTERS' AS (doc_id:chararray, norm_sq:double, vector:bag {t:tuple (token:chararray, weight:double)});

--
-- Generate similarities
--
vectors_flat = FOREACH vectors   GENERATE doc_id, norm_sq, FLATTEN(vector) AS (token, weight);
centers_flat = FOREACH k_centers GENERATE doc_id, norm_sq, FLATTEN(vector) AS (token, weight);
common_token = JOIN centers_flat BY token, vectors_flat BY token;
intersection = FOREACH common_token GENERATE
                 centers_flat::doc_id  AS center_id,
                 centers_flat::norm_sq AS center_norm_sq,
                 vectors_flat::doc_id  AS doc_id,
                 vectors_flat::norm_sq AS doc_norm_sq,
                 vectors_flat::token   AS token,
                 centers_flat::weight*vectors_flat::weight AS product
               ;

grouped_pairs = GROUP intersection BY (center_id, doc_id);
similarities  = FOREACH grouped_pairs {
                  divisor_sq = MAX(intersection.center_norm_sq)*MAX(intersection.doc_norm_sq);
                  similarity = ((double)SUM(intersection.product))/SQRT(divisor_sq);
                  GENERATE
                    FLATTEN(group) AS (center_id, doc_id),
                    similarity     AS similarity
                  ;
                };


--
-- Get the nearest center associated with each document and reattach the vectors.
-- FIXME: See below for why it's necessary to use a filter
--
finding_nearest_1 = COGROUP similarities BY doc_id INNER, vectors_flat BY doc_id INNER;
finding_nearest_2 = FILTER finding_nearest_1 BY similarities IS NOT NULL;
only_nearest    = FOREACH finding_nearest_2 {
                    --
                    -- FIXME: Ocassionally, TOP throws an NPE
                    -- see: http://issues.apache.org/jira/browse/PIG-2031
                    --
                    nearest_center = TOP(1, 2, similarities);
                    GENERATE
                      FLATTEN(nearest_center)       AS (center_id, doc_id, similarity),
                      vectors_flat.(token, weight)  AS vector 
                    ;
                  };

STORE only_nearest INTO '$NEXT_CENTERS-only_nearest';
-- 
-- --
-- -- Count the number of documents associated with each center
-- --
-- center_grpd = GROUP only_nearest BY center_id;
-- center_cnts = FOREACH center_grpd GENERATE FLATTEN(only_nearest.(center_id, vector)) AS (center_id, vector), COUNT(only_nearest) AS num_vectors;
-- 
-- for_guestimate = FOREACH center_grpd GENERATE FLATTEN(only_nearest.(center_id, doc_id)) AS (center_id, doc_id);
-- STORE for_guestimate INTO '$NEXT_CENTERS-checkpoint';
-- 
-- --
-- -- Get new counts
-- --
-- cut_nearest    = FOREACH center_cnts GENERATE center_id AS center_id, num_vectors AS num_vectors, FLATTEN(vector) AS (token:chararray, weight:double);
-- centroid_start = GROUP cut_nearest BY (center_id, token);
-- weight_avgs    = FOREACH centroid_start {
--                    weight_avg    = (double)SUM(cut_nearest.weight)/(double)MAX(cut_nearest.num_vectors);
--                    weight_avg_sq = ((double)SUM(cut_nearest.weight)/(double)MAX(cut_nearest.num_vectors))*(double)SUM(cut_nearest.weight)/(double)MAX(cut_nearest.num_vectors);
--                    GENERATE
--                      FLATTEN(group) AS (center_id, token),
--                      weight_avg     AS tf_idf,
--                      weight_avg_sq  AS tf_idf_sq
--                    ;
--                  };
--                    
-- centroids_grp  = GROUP weight_avgs BY center_id;
-- 
-- --
-- -- Need to keep from having humungous vectors
-- --
-- centroids      = FOREACH centroids_grp {
--                    --
--                    -- FIXME: Ocassionally, TOP throws an NPE
--                    -- see: http://issues.apache.org/jira/browse/PIG-2031
--                    --        
--                    ordrd            = ORDER weight_avgs BY tf_idf DESC;  
--                    shortened_vector = LIMIT ordrd $MAX_CENTER_SIZE;
--                    norm_sq          = SUM(shortened_vector.tf_idf_sq);
--                    GENERATE
--                      group            AS center_id,
--                      norm_sq          AS norm_sq,
--                      shortened_vector.($1, $2) AS vector
--                    ;
--                  };
-- 
-- STORE centroids INTO '$NEXT_CENTERS';
