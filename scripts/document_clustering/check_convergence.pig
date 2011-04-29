register '../../target/varaha-1.0-SNAPSHOT.jar';
register '../../lib/lucene-core-3.1.0.jar';

prior_centers  = LOAD '$PRIOR_CENTERS'  AS (doc_id:chararray, vector:bag {t:tuple (token:chararray, weight:double)});
latest_centers = LOAD '$LATEST_CENTERS' AS (doc_id:chararray, vector:bag {t:tuple (token:chararray, weight:double)});

together       = JOIN prior_centers BY doc_id, latest_centers BY doc_id;
for_comparison = FOREACH together {
                   diff    = varaha.text.TermVectorSimilarity(prior_centers::vector, latest_centers::vector);
                   GENERATE
                     diff AS diff
                   ;
                 };

-- 'Multiple outputs' bug with Pig
for_comp_foobar = FOREACH for_comparison GENERATE diff AS diff, diff*diff AS diff_sq;

grouped = GROUP for_comp_foobar ALL;
stats   = FOREACH grouped {
            mean_sq_diff = SQRT((double)(SUM(for_comp_foobar.diff_sq) / (double)COUNT(for_comp_foobar)));
            GENERATE
              mean_sq_diff  AS mean_sq_diff
            ;
          };
DUMP stats;
