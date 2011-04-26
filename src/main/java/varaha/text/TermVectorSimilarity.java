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

import org.apache.pig.builtin.Base;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;

/**
 * TermVectorSimilarity calculates the cosine similarity between two
 * term vectors represented as Pig bags.
 *  
 * <dt><b>Example:</b></dt>
 * <dd><code>
 * register varaha.jar;<br/>
 * vectors      = LOAD 'vectors'   AS (doc_id:chararray, vector:bag {t:tuple (token:chararray, weight:double)});<br/>
 * k_centers    = LOAD 'k_centers' AS (doc_id:chararray, vector:bag {t:tuple (token:chararray, weight:double)});<br/>
 * with_centers = CROSS k_centers, vectors;
 * similarities = FOREACH with_centers GENERATE varaha.text.TermVectorSimilarity(k_centers::vector, vectors::vector) AS (sim:double);<br/>
 * </code></dd>
 * </dl>
 * 
 * @see
 * @author Jacob Perkins
 *
 */
public class TermVectorSimilarity extends Base {

    public Double exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2 || input.isNull(0) || input.isNull(1))
            return null;
        
        TermVector t1 = new TermVector((DataBag)input.get(0));
        TermVector t2 = new TermVector((DataBag)input.get(1));
        
        return t1.cosineSimilarity(t2);
    }

}
