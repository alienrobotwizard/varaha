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

--
-- Load the vectors from the tfidf process.
--
vectors = LOAD '$TFIDF' AS (doc_id:chararray, vector:bag {t:tuple (token:chararray, weight:double)});

--
-- Choose K random centers. This is kind of a hacky process. Since we can't really use
-- parameters for the sampler we have to precompute S. Here a good heuristic for choosing S
-- is S=(K+10)/NDOCS where NDOCS is the number of documents in the input corpus. This way
-- we're "guaranteed" to get greater than (but not too much so) K vectors. Then we limit
-- it to K.

sampled   = SAMPLE vectors $S;
k_centers = LIMIT sampled $K;

STORE k_centers INTO '$CENTERS';
