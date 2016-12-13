package com.bloomberg.news.solr.search.xml;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.xml.CoreParser;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.TestCoreParser;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Query;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;

public class TestDisjunctionMaxQueryBuilder extends TestCoreParser {

  private class CoreParserDisjunctionMax extends CoreParser {
    public CoreParserDisjunctionMax(String defaultField, Analyzer analyzer) {
      super(defaultField, analyzer);

      // the query builder to be tested
      addQueryBuilder("DisjunctionMaxQuery", new DisjunctionMaxQueryBuilder(
          defaultField, analyzer, null, queryFactory));

      // some additional builders to help
      addQueryBuilder("TermQuery", TermFreqBuildersWrapper.newTermQueryBuilder(
          defaultField, analyzer, queryFactory));
      addSpanQueryBuilder("WildcardNearQuery",
          new WildcardNearQueryBuilder(defaultField, analyzer, null, spanFactory));
    }
  }

  @Override
  protected CoreParser newCoreParser(String defaultField, Analyzer analyzer) {
    return new CoreParserDisjunctionMax(defaultField, analyzer);
  }

  @Override
  protected Query parse(String xmlFileName) throws ParserException, IOException {
    try (InputStream xmlStream = TestDisjunctionMaxQueryBuilder.class.getResourceAsStream(xmlFileName)) {
      if (xmlStream == null) {
        return super.parse(xmlFileName);
      }
      assertNotNull("Test XML file " + xmlFileName + " cannot be found", xmlStream);
      Query result = coreParser().parse(xmlStream);
      return result;
    }
  }

  public void testDisjunctionMaxQuery() throws Exception {

    final float tieBreakerMultiplier = random().nextFloat();
    final boolean madQueryOnly = random().nextBoolean();
    final String xml = "<DisjunctionMaxQuery fieldName='content' tieBreaker='"+tieBreakerMultiplier+"'>"
        + (madQueryOnly ? "" : "<TermQuery fieldName='title'>guide</TermQuery>")
        + "<MatchAllDocsQuery/>"
        + "</DisjunctionMaxQuery>";

    final Query expectedQuery;
    if (madQueryOnly) {
      expectedQuery = new MatchAllDocsQuery();
    } else {
      Collection<Query> queries = new ArrayList<Query>(1);
      queries.add(new TermQuery(new Term("title", "guide")));
      final DisjunctionMaxQuery dmQuery = new DisjunctionMaxQuery(queries, tieBreakerMultiplier);
      expectedQuery = dmQuery;
    }

    final Query actualQuery = coreParser().parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
    assertEquals(expectedQuery, actualQuery);
  }

  public void testDisjunctionMaxQueryTripleWildcardNearQuery() throws Exception {
    Query q = parse("DisjunctionMaxQueryTripleWildcardNearQuery.xml");
    int size = ((DisjunctionMaxQuery)q).getDisjuncts().size();
    assertTrue("Expecting 2 clauses, but resulted in " + size, size == 2);
    DisjunctionMaxQuery dm = (DisjunctionMaxQuery)q;
    for(Query q1 : dm.getDisjuncts())
    {
      assertFalse("Not expecting MatchAllDocsQuery ",q1 instanceof MatchAllDocsQuery);
    }
  }

  public void testDisjunctionMaxQueryMatchAllDocsQuery() throws Exception {
    final Query q = parse("DisjunctionMaxQueryMatchAllDocsQuery.xml");
    assertTrue("Expecting a MatchAllDocsQuery, but resulted in " + q.getClass(), q instanceof MatchAllDocsQuery);
  }
}
