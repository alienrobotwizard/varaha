package varaha.text;

import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;

/**
 * TermVector is a wrapper around a Pig DataBag
 */
public class TermVector implements Iterable<Tuple>{

    private static DataBag vector;
    private static Double norm;
    
    public TermVector() {
        this(BagFactory.getInstance().newDefaultBag());
    }

    public TermVector(DataBag vector) {
        this.vector = vector;
    }

    public Iterator<Tuple> iterator() {
        return vector.iterator();
    }

    public DataBag toDataBag() {
        return vector;
    }

    /**
       Computes the cosine similarity between this and another term vector.

       @param other: Another TermVector

       @return the cosine similarity between the this and the other term vector
     */
    public Double cosineSimilarity(TermVector other) throws ExecException {
        return dotProduct(other)/(norm()*other.norm());
    }
    
    /**
       Returns the scalar inner product of this and the other term vector by
       multiplying each entry for the same term.
       <p>
       There are undoubtedly ways to optimize this. Please, enlighten me.

       @param other: Another term vector

       @return the dot product
     */
    public Double dotProduct(TermVector other) throws ExecException {
        Double result = 0.0;
        for (Tuple x_i : this) {
            for (Tuple y_i : other) {
                if (x_i.get(0).toString().equals(y_i.get(0).toString())) {
                    result += (Double)x_i.get(1)*(Double)y_i.get(1);
                }
            }
        }
        return result;
    }

    /**
       Computes the norm of this vector.

       @return the norm of this vector
     */
    public Double norm() throws ExecException {
        if (norm != null) {
            return norm;
        } else {
            Double result  = 0.0;
            for (Tuple x_i : vector) {
                result += (Double)x_i.get(1)*(Double)x_i.get(1);
            }
            this.norm = Math.sqrt(result);
            return norm;
        }
    }    
}
