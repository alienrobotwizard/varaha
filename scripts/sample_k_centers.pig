--
-- Load the vectors from the tfidf process.
--
vectors = LOAD '$TFIDF-vectors' AS (doc_id:chararray, vector:bag {t:tuple (token:chararray, weight:double)});

--
-- Choose K random centers. This is kind of a hacky process. Since we can't really use
-- parameters for the sampler we have to precompute S. Here a good heuristic for choosing S
-- is S=(K+10)/NDOCS where NDOCS is the number of documents in the input corpus. This way
-- we're "guaranteed" to get greater than (but not too much so) K vectors. Then we limit
-- it to K.

sampled   = SAMPLE vectors $S;
k_centers = LIMIT sampled $K;

STORE k_centers INTO '$TFIDF-centers-0';
