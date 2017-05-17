/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.search.grouping.collector;


import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.grouping.function.FunctionSecondPassGroupingCollector;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.solr.search.AbstractReRankQuery;
import org.apache.solr.search.RankQuery;

public class ReRankFunctionSecondPassGroupingCollector extends FunctionSecondPassGroupingCollector {

  /**
   * Constructs a {@link ReRankFunctionSecondPassGroupingCollector} instance.
   *
   * @param searchGroups The {@link SearchGroup} instances collected during the first phase.
   * @param groupSort The group sort
   * @param withinGroupSort The sort inside a group
   * @param query The rankquery used to rerank
   * @param searcher The index searcher
   * @param maxDocsPerGroup The maximum number of documents to collect inside a group
   * @param getScores Whether to include the scores
   * @param getMaxScores Whether to include the maximum score
   * @param fillSortFields Whether to fill the sort values in {@link TopGroups#withinGroupSort}
   * @param groupByVS The {@link ValueSource} to group by
   * @param vsContext The value source context
   * @throws IOException IOException When I/O related errors occur
   */
  public ReRankFunctionSecondPassGroupingCollector(Collection<SearchGroup<MutableValue>> searchGroups, 
                                                   Sort groupSort,
                                                   Sort withinGroupSort,
                                                   RankQuery query,
                                                   IndexSearcher searcher,
                                                   int maxDocsPerGroup,
                                                   boolean getScores,
                                                   boolean getMaxScores,
                                                   boolean fillSortFields,
                                                   ValueSource groupByVS,
                                                   Map<?, ?> vsContext) throws IOException {
    super(searchGroups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields, groupByVS, vsContext);
    final int reRankDocs;
    if (query instanceof AbstractReRankQuery){
      reRankDocs = ((AbstractReRankQuery)query).getReRankDocs();
    } else {
      // if we don't know how many documents to reorder in the group (should not happen) just reorder the
      // documents in the group
      reRankDocs = maxDocsPerGroup;
    }
    for (SearchGroup<MutableValue> group : searchGroups) {
      TopDocsCollector<?> collector;
      if (query != null) {
        collector = query.getTopDocsCollector(reRankDocs, groupSort, searcher);
        groupMap.put(group.groupValue, new SearchGroupDocs<MutableValue>(group.groupValue, collector));
      }
    }
  }
}
