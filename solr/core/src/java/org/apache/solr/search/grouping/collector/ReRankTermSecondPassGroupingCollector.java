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
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.grouping.AnchorComparator;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.grouping.term.TermSecondPassGroupingCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.search.AbstractReRankQuery;
import org.apache.solr.search.RankQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReRankTermSecondPassGroupingCollector extends TermSecondPassGroupingCollector {

  private static final Logger logger = LoggerFactory.getLogger(ReRankTermSecondPassGroupingCollector.class);

  public ReRankTermSecondPassGroupingCollector(String groupField, 
                                               Collection<SearchGroup<BytesRef>> groups,
                                               Sort groupSort, 
                                               Sort withinGroupSort, 
                                               IndexSearcher searcher, 
                                               RankQuery query, 
                                               int maxDocsPerGroup, 
                                               boolean getScores, 
                                               boolean getMaxScores,
                                               boolean fillSortFields) throws IOException {
    super(groupField, groups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
    final int reRankDocs;
    if (query instanceof AbstractReRankQuery){
      reRankDocs = ((AbstractReRankQuery)query).getReRankDocs();
    } else {
      // if we don't know how many documents to reorder in the group (should not happen) just reorder the
      // documents in the group
      reRankDocs = maxDocsPerGroup;
    }
    for (SearchGroup<BytesRef> group : groups) {
      TopDocsCollector<?> collector;
      if (query != null) {
        collector = query.getTopDocsCollector(reRankDocs, groupSort, searcher);
        groupMap.put(group.groupValue, new SearchGroupDocs<BytesRef>(group.groupValue, collector));
      }
    }
  }

  public ReRankTermSecondPassGroupingCollector(String groupField, 
                                               Collection<SearchGroup<BytesRef>> groups,
                                               Sort groupSort, 
                                               Sort withinGroupSort,
                                               IndexSearcher searcher,
                                               RankQuery query,
                                               int maxDocsPerGroup,
                                               boolean getScores,
                                               boolean getMaxScores,
                                               boolean fillSortFields,
                                               boolean forward,
                                               AnchorComparator anchorComparator) throws IOException {
    super(groupField, groups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields, forward,  anchorComparator );
    final int reRankDocs;
    if (query instanceof AbstractReRankQuery){
      reRankDocs = ((AbstractReRankQuery)query).getReRankDocs();
    } else {
      // if we don't know how many documents to reorder in the group (should not happen) just reorder the
      // documents in the group
      reRankDocs = maxDocsPerGroup;
    }
    for (SearchGroup<BytesRef> group : groups) {
      TopDocsCollector<?> collector;
      if (query != null) {
        collector = query.getTopDocsCollector(reRankDocs, groupSort, searcher);
        groupMap.put(group.groupValue, new SearchGroupDocs<BytesRef>(group.groupValue, collector));
      }
    }
  }
}
