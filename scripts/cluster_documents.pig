register 'target/varaha-1.0-SNAPSHOT.jar';

vectors = LOAD '$TFIDF-vectors' AS (doc_id:chararray, vector:bag {t:tuple (token:chararray, weight:double)});

--
-- Choose K random centers. This is kind of a hacky process. Since we can't really use
-- parameters for the sampler we have to precompute S. Here S=(K+5)/NDOCS. This way we're
-- guaranteed to get greater than (but not too much so) K vectors. Then we limit it to K.
--
-- sampled   = SAMPLE vectors $S;
-- k_centers = LIMIT sampled $K;
-- 
-- STORE k_centers INTO '$TFIDF-centers-0';

-- k_centers    = LOAD '$TFIDF-centers-0' AS (doc_id:chararray, vector:bag {t:tuple (token:chararray, weight:double)});
-- with_centers = CROSS k_centers, vectors;
-- similarities = FOREACH with_centers GENERATE
--                  k_centers::doc_id AS center_id,
--                  k_centers::vector AS center,
--                  vectors::doc_id   AS doc_id,
--                  vectors::vector   AS vector,
--                  varaha.text.TermVectorSimilarity(k_centers::vector, vectors::vector) AS similarity;

-- STORE similarities INTO '$TFIDF-similarities-0';
-- similarities = LOAD '$TFIDF-similarities-0' AS (
--                  center_id:chararray,
--                  center:bag {t:tuple (token:chararray, weight:double)},
--                  doc_id:chararray,
--                  vector:bag {t:tuple (token:chararray, weight:double)},
--                  similarity:double
--                );
-- 
-- finding_nearest = GROUP similarities BY doc_id;
-- only_nearest    = FOREACH finding_nearest {
--                     nearest_center = TOP(1, 4, similarities);
--                     GENERATE
--                       FLATTEN(nearest_center) AS (center_id, center, doc_id, vector, similarity)
--                     ;
--                   };
-- cut_nearest     = FOREACH only_nearest GENERATE center_id, vector;
-- clusters        = GROUP cut_nearest BY center_id; -- this gets worse as K/NDOCS gets smaller
-- 
-- cut_clusters    = FOREACH clusters GENERATE group AS center_id, cut_nearest.vector AS vector_collection;
-- STORE cut_clusters INTO '$TFIDF-clusters-0';

clusters = LOAD '$TFIDF-clusters-0' AS (center_id:chararray, vectors:bag {t:tuple (vector:bag {s:tuple (token:chararray, weight:double)})});
centroids       = FOREACH clusters GENERATE
                    center_id,
                    varaha.text.TermVectorCentroid(vectors) -- implement this
                  ;
STORE centroids INTO '$TFIDF-centroids-0';
