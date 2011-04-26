package varaha.text;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * TokenizeText uses the Lucene libraries StandardAnalyzer class to tokenize a
 * raw text input. Output is a pig bag containing tokens.
 *  
 * <dt><b>Example:</b></dt>
 * <dd><code>
 * register varaha.jar;<br/>
 * documents    = LOAD 'documents' AS (doc_id:chararray, text:chararray);<br/>
 * tokenized    = FOREACH documents GENERATE doc_id AS doc_id, FLATTEN(TokenizeText(text)) AS (token:chararray);
 * </code></dd>
 * </dl>
 * 
 * @see
 * @author Jacob Perkins
 *
 */
public class TokenizeText extends EvalFunc<DataBag> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();
    private static String NOFIELD = "";
    private static StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_31);

    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1 || input.isNull(0))
            return null;

        // Output bag
        DataBag bagOfTokens = bagFactory.newDefaultBag();
                
        StringReader textInput = new StringReader(input.get(0).toString());
        TokenStream stream = analyzer.tokenStream(NOFIELD, textInput);
        CharTermAttribute termAttribute = stream.getAttribute(CharTermAttribute.class);

        while (stream.incrementToken()) {
            Tuple termText = tupleFactory.newTuple(termAttribute.toString());
            bagOfTokens.add(termText);
            termAttribute.setEmpty();
        }
        return bagOfTokens;
    }
}
