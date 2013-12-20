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
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;

import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.pattern.PatternReplaceFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * TokenizeText uses the Lucene library's StandardAnalyzer class to tokenize a
 * raw text input. A list of the stopwords used is available {@link StopWords}.
 * Output is a pig data bag containing tokens.
 *  
 * <dt><b>Example:</b></dt>
 * <dd><code>
 * register varaha.jar;<br/>
 *
 * define TokenizeText varaha.text.TokenizeText('$minGramSize', '$maxGramSize', '$minWordSize');
 *
 * documents = load 'documents' as (doc_id:chararray, text:chararray);<br/>
 * tokenized = foreach documents generate doc_id, flatten(TokenizeText(text)) as (token:chararray);
 * </code></dd>
 * </dl>
 *
 *
 * @param minGramSize Minimum number of individual terms to include in the returned ngrams
 * @param maxGramSize Maximum number of individual terms to include in the returned ngrams
 * @param minWordSize Minimum number of characters allowed per term in the returned ngrams
 *
 * @see
 * @author Jacob Perkins
 */
public class TokenizeText extends EvalFunc<DataBag>{

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();
    private static String NOFIELD = "";
    private static Pattern SHINGLE_FILLER = Pattern.compile(".* _ .*|_ .*|.* _| _");
    private static StandardAnalyzer analyzer;
    
    private Integer minWordSize;
    private Integer minGramSize;
    private Integer maxGramSize;
    private Boolean outputUnigrams;
    
    public TokenizeText(String minGramSize, String maxGramSize) {
        this(minGramSize, maxGramSize, "3");
    }
    
    public TokenizeText(String minGramSize, String maxGramSize, String minWordSize) {
        this.minWordSize = Integer.parseInt(minWordSize);
        this.minGramSize = Integer.parseInt(minGramSize);
        this.maxGramSize = Integer.parseInt(maxGramSize);
        this.analyzer = new StandardAnalyzer(Version.LUCENE_44, StopWords.ENGLISH_STOP_WORDS);
        validateSizes();
    }

    public void validateSizes() {
        outputUnigrams = false;
        if (minGramSize == 1 && maxGramSize > 1) {
            minGramSize = 2;
            outputUnigrams = true;
        }
    }
    
    /**
       Uses Lucene's StandardAnalyzer and tuns the tokens through several lucene filters
       - LengthFilter: Filter individual words to be of length > minWordSize
       - ShingleFilter: Converts word stream into n-gram stream
       - PatternReplaceFilter: Removes the 'filler' character that ShingleFilter puts in to
         replace stopwords
     */
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1 || input.isNull(0))
            return null;
        
        TokenStream stream = analyzer.tokenStream(NOFIELD, input.get(0).toString());
        LengthFilter filtered = new LengthFilter(Version.LUCENE_44, stream, minWordSize, Integer.MAX_VALUE); // Let words be long

        DataBag result;
        if (minGramSize == 1 && maxGramSize == 1) {
            result = fillBag(filtered);
        } else {
            ShingleFilter nGramStream = new ShingleFilter(filtered, minGramSize, maxGramSize);        
            nGramStream.setOutputUnigrams(outputUnigrams);                
            PatternReplaceFilter replacer = new PatternReplaceFilter(nGramStream, SHINGLE_FILLER, NOFIELD, true);
            result = fillBag(replacer);
        }
        return result;
    }

    /**
       Fills a DataBag with tokens from a TokenStream
     */
    public DataBag fillBag(TokenStream stream) throws IOException {
        DataBag result = bagFactory.newDefaultBag();
        CharTermAttribute termAttribute = stream.addAttribute(CharTermAttribute.class);
        try {
            stream.reset();
            while (stream.incrementToken()) {
                if (termAttribute.length() > 0) {
                    Tuple termText = tupleFactory.newTuple(termAttribute.toString());
                    result.add(termText);
                }
            }
            stream.end();
        } finally {            
            stream.close();
        }
        return result;
    }
}


// package varaha.text;
// 
// import java.io.IOException;
// import java.io.StringReader;
// import java.util.Iterator;
// import java.util.Set;
// import java.util.HashSet;
// 
// import org.apache.pig.EvalFunc;
// import org.apache.pig.data.Tuple;
// import org.apache.pig.data.TupleFactory;
// import org.apache.pig.data.DataBag;
// import org.apache.pig.data.BagFactory;
// 
// import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
// import org.apache.lucene.util.Version;
// import org.apache.lucene.analysis.Token;
// import org.apache.lucene.analysis.TokenStream;
// import org.apache.lucene.analysis.standard.StandardAnalyzer;
// import org.apache.lucene.analysis.standard.StandardTokenizer;
// 
// /**
//  *
//  */
// public class TokenizeText extends EvalFunc<DataBag> {
// 
//     private static TupleFactory tupleFactory = TupleFactory.getInstance();
//     private static BagFactory bagFactory = BagFactory.getInstance();
//     private static String NOFIELD = "";
//     private static StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_31, StopWords.ENGLISH_STOP_WORDS);
// 
//     public DataBag exec(Tuple input) throws IOException {
//         if (input == null || input.size() < 1 || input.isNull(0))
//             return null;
// 
//         // Output bag
//         DataBag bagOfTokens = bagFactory.newDefaultBag();
//                 
//         StringReader textInput = new StringReader(input.get(0).toString());
//         TokenStream stream = analyzer.tokenStream(NOFIELD, textInput);
//         CharTermAttribute termAttribute = stream.getAttribute(CharTermAttribute.class);
// 
//         while (stream.incrementToken()) {
//             Tuple termText = tupleFactory.newTuple(termAttribute.toString());
//             bagOfTokens.add(termText);
//             termAttribute.setEmpty();
//         }
//         return bagOfTokens;
//     }
// }
