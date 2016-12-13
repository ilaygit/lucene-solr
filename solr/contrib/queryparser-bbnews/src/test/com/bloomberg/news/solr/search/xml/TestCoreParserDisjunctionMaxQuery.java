package com.bloomberg.news.solr.search.xml;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.io.InputStream;

public class TestCoreParserDisjunctionMaxQuery extends TestCoreParser {

  private class CoreParserDisjunctionMaxQuery extends CoreParser {
    CoreParserDisjunctionMaxQuery(String defaultField, Analyzer analyzer) {
      super(defaultField, analyzer);

      // the query builder to be tested
      addQueryBuilder("DisjunctionMaxQuery", new DisjunctionMaxQueryBuilder(
        defaultField, analyzer, null, queryFactory));

      // some additional builders to help
      // (here only since requiring access to queryFactory)
      addSpanQueryBuilder("WildcardNearQuery", new WildcardNearQueryBuilder(
        defaultField, analyzer, null, spanFactory));
    }
  }

  @Override
  protected CoreParser newCoreParser(String defaultField, Analyzer analyzer) {
    final CoreParser coreParser = new CoreParserDisjunctionMaxQuery(defaultField, analyzer);

    return coreParser;
  }

  @Override
  protected Query parse(String xmlFileName) throws ParserException, IOException {
    try (InputStream xmlStream = TestCoreParserDisjunctionMaxQuery.class.getResourceAsStream(xmlFileName)) {
      if (xmlStream == null) {
        return super.parse(xmlFileName);
      }
      Query result = coreParser().parse(xmlStream);
      return result;
    }
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
