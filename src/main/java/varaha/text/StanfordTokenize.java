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

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.process.PTBTokenizer;

/**
 * TokenizeText uses the Lucene libraries StandardAnalyzer class to tokenize a
 * raw text input. A list of the stopwords used is available {@link StopWords}.
 * Output is a pig bag containing tokens.
 *  
 * <dt><b>Example:</b></dt>
 * <dd><code>
 * register varaha.jar;<br/>
 * documents    = LOAD 'documents' AS (doc_id:chararray, text:chararray);<br/>
 * tokenized    = FOREACH documents GENERATE doc_id AS doc_id, FLATTEN(StanfordTokenize(text)) AS (token:chararray);
 * </code></dd>
 * </dl>
 * 
 * @see
 * @author Russell Jurney
 *
 */
public class StanfordTokenize extends EvalFunc<DataBag> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();

    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1 || input.isNull(0))
            return null;

        // Output bag
        DataBag bagOfTokens = bagFactory.newDefaultBag();
                
        StringReader textInput = new StringReader(input.get(0).toString());
        PTBTokenizer ptbt = new PTBTokenizer(textInput, new CoreLabelTokenFactory(), "");

        for (CoreLabel label; ptbt.hasNext(); ) {
          label = (CoreLabel)ptbt.next();
          if(label.value().length() > 2)
          {
            System.err.println(label.toString());
            Tuple termText = tupleFactory.newTuple(label.word());
            bagOfTokens.add(termText);
          }
        }  
        return bagOfTokens;
    }
}
