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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.process.DocumentPreprocessor;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.ling.Word;

/**
 * StanfordPOSTagger uses the Stanford Maximum Entropy Tagger class to Part-Of-Speech tag a
 * raw text input. Output is a pig bag containing two-field tuples, of the format (word, tag).
 *
 * <dt><b>Example:</b></dt>
 * <dd><code>
 * register varaha.jar;<br/>
 * documents    = LOAD 'documents' AS (doc_id:chararray, text:chararray);<br/>
 * tokenized    = FOREACH documents GENERATE doc_id AS doc_id, StanfordPOSTagger(text)
 *                                  AS (b:bag{token:tuple(word:chararray, tag:chararray)});
 * </code></dd>
 * </dl>
 *
 * @see
 * @author Russell Jurney
 *
 */
public class StanfordPOSTag extends EvalFunc<DataBag> {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();
    private static boolean isFirst = true;
    private static MaxentTagger tagger;

    // Must also add implementation for bag sof tuples of sentences
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1 || input.isNull(0))
            return null;

        if(isFirst)
        {
            try {
                tagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger");
            }
            catch(Exception e) {
                System.err.println("Exception loading language model: " + e.getMessage());
            }
            isFirst = false;
        }

        // Output bag
        DataBag bagOfTokens = bagFactory.newDefaultBag();

        Object inThing = input.get(0);
        if(inThing instanceof String) {
            StringReader textInput = new StringReader((String)inThing);
            Tuple termText = null;
            List<TaggedWord> taggedSentence = null;
            DocumentPreprocessor dp = new DocumentPreprocessor(textInput);
            for (List sentence : dp) {
                taggedSentence = tagger.apply(sentence);

                // Now split based on '_' and build/return a bag of 2-field tuples
                termText = tupleFactory.newTuple();
                for (TaggedWord word : taggedSentence ) {
                    String token = word.word();
                    String tag = word.tag();
                    termText = tupleFactory.newTuple(Arrays.asList(token, tag));
                    bagOfTokens.add(termText);
                }
            }
            bagOfTokens.add(termText);
        }
        else if(inThing instanceof DataBag) {
            Iterator<Tuple> itr = ((DataBag)inThing).iterator();
            List<Word> sentence = new ArrayList<Word>();
            while(itr.hasNext()) {
                Tuple t = itr.next();
                if(t.get(0) != null) {
                    Word word = new Word(t.get(0).toString());
                    sentence.add(word);
                }
            }
            List<TaggedWord> taggedSentence = tagger.apply(sentence);
            for( TaggedWord word : taggedSentence) {
                String token = word.word();
                String tag = word.tag();
                Tuple termText = tupleFactory.newTuple(Arrays.asList(token, tag));
                bagOfTokens.add(termText);
            }
        }
        else
        {
            throw new IOException();
        }

        return bagOfTokens;
    }
}
