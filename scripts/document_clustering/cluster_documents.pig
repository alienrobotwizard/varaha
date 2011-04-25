register '../../target/varaha-1.0-SNAPSHOT.jar'; -- yikes, just autoregister this in the runner

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
