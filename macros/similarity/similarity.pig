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
        
 /*
  * Given a relation of ids and their associated feature vectors, returns the id-id
  * edges pairs and their cosine similarities. Only id-id pairs with a non-zero
  * edge weight are returned.        
  *
  * feature_vectors:   { id, features:{(dimension, weight:float)} }
  *   where *dimension* can be any type. In the document case *dimension* is the
  *   ngram.
  * ==>
  * graph: { idA, idB, cosine_simiarity:float }
 */
define VarahaSimilarityCosine(feature_vectors) returns graph {

  --
  -- Normalize the vectors
  --
  vectors = foreach $feature_vectors generate $0 as id, $1 as features:bag{t:tuple(dim, weight:double)};
  vectors = foreach vectors {
              squares        = foreach features generate weight*weight as weight_squared;
              squares_summed = SUM(squares.weight_squared);
              magnitude      = SQRT(squares_summed);
              generate
                id                as id,
                flatten(features) as (dim,weight),
                magnitude         as magnitude;
            };
  
  normalizedA = foreach vectors generate id as id, dim as dim, weight/magnitude as weight;
  normalizedB = foreach normalizedA generate id as id, dim as dim, weight as weight; -- again, for self join
  
  --
  -- Get pairs of documents that have at least one dim in common
  --
  intersect = cogroup normalizedA by dim, normalizedB by dim;
  intersect = foreach (filter intersect by not IsEmpty(normalizedA) and not IsEmpty(normalizedB)) {
                pairs = cross normalizedA.(id,weight), normalizedB.(id,weight);
                pairs = filter pairs by $0 != $2; -- don't consider self-loops
                dots  = foreach pairs generate $0 as idA, $2 as idB, $1*$3 as product;
                generate
                  flatten(dots) as (idA, idB, product);
              };
  
   --
   -- Compute similarity scores
   --
   $graph = foreach (group intersect by (idA, idB)) {
              sim = SUM(intersect.product);
              generate
                flatten(group) as (idA, idB),
                sim            as cosine_similarity;
            };
};
