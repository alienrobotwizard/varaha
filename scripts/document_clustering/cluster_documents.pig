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
-- Group on center_id and collect all the documents associated with each center. This
-- can be quite memory intensive and gets nearly impossible when K/NDOCS is a small number.
--
cut_nearest     = FOREACH only_nearest GENERATE center_id, vector;
clusters        = GROUP cut_nearest BY center_id; -- this gets worse as K/NDOCS gets smaller 
cut_clusters    = FOREACH clusters GENERATE group AS center_id, cut_nearest.vector AS vector_collection;

--
-- Compute the centroid of all the documents associated with a given center. These will be the new
-- centers in the next iteration.
--
centroids       = FOREACH cut_clusters GENERATE
                    group AS center_id,
                    varaha.text.TermVectorCentroid(vector_collection)
                  ;

STORE centroids INTO '$NEXT_CENTERS';
