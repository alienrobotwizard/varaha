/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package varaha.text;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;

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
 * raw text input. A list of the stopwords used is available {@link StopWords}.
 * Output is a pig bag containing tokens.
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
    private static StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_31, StopWords.ENGLISH_STOP_WORDS);

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
