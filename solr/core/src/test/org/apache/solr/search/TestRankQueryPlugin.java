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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.request.SolrQueryRequest;

import org.junit.Ignore;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Ignore
public class TestRankQueryPlugin extends QParserPlugin {


  public void init(NamedList params) {

  }

  public QParser createParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new TestRankQueryParser(query, localParams, params, req);
  }

  class TestRankQueryParser extends QParser {

    public TestRankQueryParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(query, localParams, params, req);
    }

    public Query parse() throws SyntaxError {

      int mergeStrategy = localParams.getInt("mergeStrategy", 0);
      int collector = localParams.getInt("collector", 0);
      return new TestRankQuery(collector, mergeStrategy);
    }
  }

  class TestRankQuery extends RankQuery {

    private int mergeStrategy;
    private int collector;
    private Query q;

    public int hashCode() {
      return collector+q.hashCode();
    }

    public boolean equals(Object o) {
      if(o instanceof TestRankQuery) {
        TestRankQuery trq = (TestRankQuery)o;

        return (trq.q.equals(q) && trq.collector == collector) ;
      }

      return false;
    }

    public Weight createWeight(IndexSearcher indexSearcher ) throws IOException{
      return q.createWeight(indexSearcher);
    }

    public void setBoost(float boost) {
      q.setBoost(boost);
    }

    public float getBoost() {
      return q.getBoost();
    }

    public String toString() {
      return q.toString();
    }

    public String toString(String field) {
      return q.toString(field);
    }

    public RankQuery wrap(Query q) {
      this.q = q;
      return this;
    }

    public TestRankQuery(int collector, int mergeStrategy) {
      this.collector = collector;
      this.mergeStrategy = mergeStrategy;
    }

    @Override
    public TopDocsCollector getTopDocsCollector(int len, SolrIndexSearcher.QueryCommand cmd, IndexSearcher searcher) {
      if(collector == 0)
        return new TestCollector(null);
      else
        return new TestCollector1(null);
    }

    @Override
    public TopDocsCollector getTopDocsCollector(int len, Sort sort, IndexSearcher searcher) {
      if(collector == 0)
        return new TestCollector(null);
      else
        return new TestCollector1(null);
    }
  }





  class TestCollector extends TopDocsCollector {

    private List<ScoreDoc> list = new ArrayList();
    private FieldCache.Ints values;
    private int base;

    public TestCollector(PriorityQueue pq) {
      super(pq);
    }

    public boolean acceptsDocsOutOfOrder() {
      return false;
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      values = FieldCache.DEFAULT.getInts(context.reader(), "sort_i", false);
      base = context.docBase;
    }

    public void setScorer(Scorer scorer) {

    }

    public void collect(int doc) {
      list.add(new ScoreDoc(doc+base, (float)values.get(doc)));
    }

    public int topDocsSize() {
      return list.size();
    }

    @Override
    public TopDocs topDocs() {
      Collections.sort(list, new Comparator() {
        public int compare(Object o1, Object o2) {
          ScoreDoc s1 = (ScoreDoc) o1;
          ScoreDoc s2 = (ScoreDoc) o2;
          if (s1.score == s2.score) {
            return 0;
          } else if (s1.score < s2.score) {
            return 1;
          } else {
            return -1;
          }
        }
      });
      ScoreDoc[] scoreDocs = list.toArray(new ScoreDoc[list.size()]);
      return new TopDocs(list.size(), scoreDocs, 0.0f);
    }

    public TopDocs topDocs(int start, int len) {
      return topDocs();
    }

    public int getTotalHits() {
      return list.size();
    }
  }

  class TestCollector1 extends TopDocsCollector {

    private List<ScoreDoc> list = new ArrayList();
    private int base;
    private Scorer scorer;

    public TestCollector1(PriorityQueue pq) {
      super(pq);
    }

    public boolean acceptsDocsOutOfOrder() {
      return false;
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      base = context.docBase;
    }

    public void setScorer(Scorer scorer) {
      this.scorer = scorer;
    }

    public void collect(int doc) throws IOException {
      list.add(new ScoreDoc(doc+base, scorer.score()));
    }

    public int topDocsSize() {
      return list.size();
    }

    public TopDocs topDocs() {
      Collections.sort(list, new Comparator() {
        public int compare(Object o1, Object o2) {
          ScoreDoc s1 = (ScoreDoc) o1;
          ScoreDoc s2 = (ScoreDoc) o2;
          if (s1.score == s2.score) {
            return 0;
          } else if (s1.score > s2.score) {
            return 1;
          } else {
            return -1;
          }
        }
      });
      ScoreDoc[] scoreDocs = list.toArray(new ScoreDoc[list.size()]);
      return new TopDocs(list.size(), scoreDocs, 0.0f);
    }

    public TopDocs topDocs(int start, int len) {
      return topDocs();
    }

    public int getTotalHits() {
      return list.size();
    }
  }




}
