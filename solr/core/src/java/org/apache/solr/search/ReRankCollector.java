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
package org.apache.solr.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.component.QueryElevationComponent;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.SolrIndexSearcher.QueryCommand;

/* A TopDocsCollector used by reranking queries. */
public class ReRankCollector extends TopDocsCollector<ScoreDoc> {

  final private TopDocsCollector  mainCollector;
  final private IndexSearcher searcher;
  final private int reRankDocs;
  final private int length;
  final private Map<BytesRef, Integer> boostedPriority;
  final private Rescorer reRankQueryRescorer;

  @Deprecated
  public ReRankCollector(int reRankDocs,
      int length,
      Rescorer reRankQueryRescorer,
      QueryCommand cmd,
      IndexSearcher searcher,
      Map<BytesRef, Integer> boostedPriority) throws IOException {
      this(reRankDocs, length, cmd.getSort(), reRankQueryRescorer, searcher, boostedPriority);
 }

 public ReRankCollector(int reRankDocs,
      int length,
      Sort sort,
      Rescorer reRankQueryRescorer,
      IndexSearcher searcher,
      Map<BytesRef, Integer> boostedPriority) throws IOException {
    super(null);
    this.reRankDocs = reRankDocs;
    this.length = length;
    this.boostedPriority = boostedPriority;
    final boolean docsScoredInOrder = true;
    if(sort == null) {
      this.mainCollector = TopScoreDocCollector.create(Math.max(this.reRankDocs, length), docsScoredInOrder);
    } else {
      sort = sort.rewrite(searcher);
      this.mainCollector = TopFieldCollector.create(sort, Math.max(this.reRankDocs, length), true, true, true, docsScoredInOrder);
    }
    this.searcher = searcher;
    this.reRankQueryRescorer = reRankQueryRescorer;
  }

  public int getTotalHits() {
    return mainCollector.getTotalHits();
  }


  public TopDocs topDocs(int start, int howMany) {

    try {

      TopDocs mainDocs = mainCollector.topDocs(0,  Math.max(reRankDocs, length));

      if(mainDocs.totalHits == 0 || mainDocs.scoreDocs.length == 0) {
        return mainDocs;
      }

      ScoreDoc[] mainScoreDocs = mainDocs.scoreDocs;
      ScoreDoc[] reRankScoreDocs = new ScoreDoc[Math.min(mainScoreDocs.length, reRankDocs)];
      System.arraycopy(mainScoreDocs, 0, reRankScoreDocs, 0, reRankScoreDocs.length);

      mainDocs.scoreDocs = reRankScoreDocs;

      TopDocs rescoredDocs = reRankQueryRescorer
          .rescore(searcher, mainDocs, mainDocs.scoreDocs.length);

      //Lower howMany to return if we've collected fewer documents.
      howMany = Math.min(howMany, mainScoreDocs.length);

      if(boostedPriority != null) {
          // We are not using QueryElevationComponent,so I'm not porting this feature
          throw new UnsupportedOperationException("QueryEvevation component not supported");
      }

      if(howMany == rescoredDocs.scoreDocs.length) {
        return rescoredDocs; // Just return the rescoredDocs
      } else if(howMany > rescoredDocs.scoreDocs.length) {
        //We need to return more then we've reRanked, so create the combined page.
        ScoreDoc[] scoreDocs = new ScoreDoc[howMany];
        System.arraycopy(mainScoreDocs, 0, scoreDocs, 0, scoreDocs.length); //lay down the initial docs
        System.arraycopy(rescoredDocs.scoreDocs, 0, scoreDocs, 0, rescoredDocs.scoreDocs.length);//overlay the re-ranked docs.
        rescoredDocs.scoreDocs = scoreDocs;
        return rescoredDocs;
      } else {
        //We've rescored more then we need to return.
        ScoreDoc[] scoreDocs = new ScoreDoc[howMany];
        System.arraycopy(rescoredDocs.scoreDocs, 0, scoreDocs, 0, howMany);
        rescoredDocs.scoreDocs = scoreDocs;
        return rescoredDocs;
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    mainCollector.setScorer(scorer);
  }

  @Override
  public void collect(int doc) throws IOException {
    mainCollector.collect(doc);
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    mainCollector.setNextReader(context);
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }
}
