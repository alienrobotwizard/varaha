package varaha.text;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;

/**
 * TermVectorCentroid calculates the centroid of term vectors.
 *  
 * <dt><b>Example:</b></dt>
 * <dd><code>
 * register varaha.jar;<br/>
 * clusters  = LOAD 'clusters' AS (center_id:chararray, vectors:bag {t:tuple (vector:bag {s:tuple (token:chararray, weight:double)})});
 * centroids = FOREACH clusters GENERATE
 *               center_id AS center_id,
 *               varaha.text.TermVectorCentroid(vectors) AS centroid:bag {t:tuple (token:chararray, weight:double)}
 *             ;
 * </code></dd>
 * </dl>
 * 
 * @see
 * @author Jacob Perkins
 *
 */
public class TermVectorCentroid extends EvalFunc<DataBag> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1 || input.isNull(0))
            return null;

        DataBag bagOfVectors = (DataBag)input.get(0);
        DataBag centroid = BagFactory.getInstance().newDefaultBag();
        HashMap termSums = new HashMap<String, Double>();

        //
        // Add each unique term to a hashmap and sum the entries
        //
        for (Tuple t : bagOfVectors) {
            DataBag v = (DataBag)t.get(0);
            for (Tuple v_i : v) {
                String term = v_i.get(0).toString();
                Object currentValue = termSums.get(term);
                if (currentValue == null) {
                    termSums.put(term, v_i.get(1));
                } else {
                    termSums.put(term, (Double)v_i.get(1) + (Double)currentValue);
                }
            }
        }

        //
        // Go back through the hashmap and make the values averages
        //
        Iterator mapIterator = termSums.entrySet().iterator();
        while (mapIterator.hasNext()) {
            Map.Entry pair = (Map.Entry)mapIterator.next();
            Tuple termWeightPair = tupleFactory.newTuple(2);
            termWeightPair.set(0, pair.getKey());
            termWeightPair.set(1, (Double)pair.getValue()/bagOfVectors.size());
            centroid.add(termWeightPair);
        }
        return centroid;
    }

}
