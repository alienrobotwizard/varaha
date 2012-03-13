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

package varaha.topic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.backend.executionengine.ExecException;

import cc.mallet.pipe.Pipe;
import cc.mallet.pipe.CharSequenceLowercase;
import cc.mallet.pipe.CharSequence2TokenSequence;
import cc.mallet.pipe.CharSequence2CharNGrams;
import cc.mallet.pipe.TokenSequenceNGrams;
import cc.mallet.pipe.TokenSequenceRemoveStopwords;
import cc.mallet.pipe.TokenSequence2FeatureSequence;
import cc.mallet.pipe.TokenSequence2FeatureSequenceWithBigrams;

import cc.mallet.types.TokenSequence;
import cc.mallet.types.Token;

import cc.mallet.pipe.SerialPipes;
import cc.mallet.types.InstanceList;
import cc.mallet.types.Instance;
import cc.mallet.types.Alphabet;
import cc.mallet.types.IDSorter;
import cc.mallet.types.LabelSequence;
import cc.mallet.topics.TopicAssignment;
import cc.mallet.topics.ParallelTopicModel;

public class LDATopics extends EvalFunc<DataBag> {

    private Pipe pipe;
    private static Long numKeywords = 50l; // Maximum number of keywords to use to describe a topic
    
    public LDATopics() {
        pipe = buildPipe();
    }
    
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2 || input.isNull(0) || input.isNull(1))
            return null;

        Integer numTopics = (Integer)input.get(0); // Number of topics to discover
        DataBag documents = (DataBag)input.get(1); // Documents, {(doc_id, text)}
        DataBag result = BagFactory.getInstance().newDefaultBag();

        InstanceList instances = new InstanceList(pipe);

        // Add the input databag as source data and run it through the pipe built
        // by the constructor.
        instances.addThruPipe(new DataBagSourceIterator(documents));

        // Create a model with numTopics, alpha_t = 0.01, beta_w = 0.01
        // Note that the first parameter is passed as the sum over topics, while
        // the second is the parameter for a single dimension of the Dirichlet prior.
        ParallelTopicModel model = new ParallelTopicModel(numTopics, 1.0, 0.01);
        model.addInstances(instances);
        model.setNumThreads(1); // Important, since this is being run in the reduce, just use one thread
        model.setTopicDisplay(0,0);
        model.setNumIterations(2000);
        model.estimate();

        // Get the results
        Alphabet dataAlphabet = instances.getDataAlphabet();
        ArrayList<TopicAssignment> assignments = model.getData();

        // Convert the results into comprehensible topics
        for (int topicNum = 0; topicNum < model.getNumTopics(); topicNum++) {
            TreeSet<IDSorter> sortedWords = model.getSortedWords().get(topicNum);
            Iterator<IDSorter> iterator = sortedWords.iterator();

            DataBag topic = BagFactory.getInstance().newDefaultBag();
            
            // Get the set of keywords with weights for this topic and add them as tuples
            // to the databag used to represent this topic
            while (iterator.hasNext() && topic.size() < numKeywords) {
                IDSorter info = iterator.next();
                Tuple weightedWord = TupleFactory.getInstance().newTuple(2);
                String wordToken = model.alphabet.lookupObject(info.getID()).toString(); // get the actual term text
                weightedWord.set(0, wordToken);
                weightedWord.set(1, info.getWeight()); // the raw weight of the term
                topic.add(weightedWord);
            }
            Tuple topicTuple = TupleFactory.getInstance().newTuple(2);
            topicTuple.set(0, topicNum);
            topicTuple.set(1, topic);
            result.add(topicTuple);
        }
        
        return result;
    }

    // Instantiates a new pipe object for ingesting pig tuples
    private Pipe buildPipe() {
        Pattern tokenPattern = Pattern.compile("\\S[\\S]+\\S");
        int[] sizes = {1,2};
        ArrayList pipeList = new ArrayList();

        pipeList.add(new CharSequence2TokenSequence(tokenPattern));
        pipeList.add(new TokenSequenceRemoveStopwords(false, false)); // we should use a real stop word list
        pipeList.add(new TokenSequenceNGramsDelim(sizes, " "));
        pipeList.add(new TokenSequence2FeatureSequence());
        return new SerialPipes(pipeList);
    }

    /**
       A few minor updates to TokenSequenceNGrams:

       (1) use delimiter that's passed in to delineate ngrams
     */
    private class TokenSequenceNGramsDelim extends TokenSequenceNGrams {
        int [] gramSizes = null;
        String delim = null;
    
	public TokenSequenceNGramsDelim(int [] sizes, String delim) {
            super(sizes);
            this.gramSizes = sizes;
            this.delim = delim;            
	}

        @Override
	public Instance pipe (Instance carrier) {
            String newTerm = null;
            TokenSequence tmpTS = new TokenSequence();
            TokenSequence ts = (TokenSequence) carrier.getData();

            for (int i = 0; i < ts.size(); i++) {
                Token t = ts.get(i);
                for(int j = 0; j < gramSizes.length; j++) {
                    int len = gramSizes[j];
                    if (len <= 0 || len > (i+1)) continue;
                    if (len == 1) { tmpTS.add(t); continue; }
                    newTerm = new String(t.getText());
                    for(int k = 1; k < len; k++)
                        newTerm = ts.get(i-k).getText() + delim + newTerm;
                    tmpTS.add(newTerm);
                }
            }
            carrier.setData(tmpTS);
            return carrier;
	}
    }

    /**
       Allow for a databag to be source data for mallet's clustering
     */
    private class DataBagSourceIterator implements Iterator<Instance> {

        private Iterator<Tuple> tupleItr;
        private String currentId;
        private String currentText;
        
        public DataBagSourceIterator(DataBag bag) {
            tupleItr = bag.iterator();
        }
        
        public boolean hasNext() {
            if (tupleItr.hasNext()) {
                Tuple t = tupleItr.next();
                try {
                    if (!t.isNull(0) && !t.isNull(1)) {
                        currentId = t.get(0).toString();
                        currentText = t.get(1).toString();
                        if (currentId.isEmpty() || currentText.isEmpty()) {
                            return false;
                        } else {
                            return true;
                        }   
                    }
                } catch (ExecException e) {
                    throw new RuntimeException(e);
                }
            }
            return false;
        }

        public Instance next() {
            // Get the next tuple and pull out its fields
            Instance i = new Instance(currentText, "X", currentId, null);
            return i;
        }
        
        public void remove() {
            tupleItr.remove();
        }
    }
}
