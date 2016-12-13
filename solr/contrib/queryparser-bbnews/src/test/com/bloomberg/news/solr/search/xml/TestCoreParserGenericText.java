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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.xml.CoreParser;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.TestCoreParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;
import java.io.InputStream;

public class TestCoreParserGenericText extends TestCoreParser {

  private class CoreParserGenericText extends CoreParser {
    CoreParserGenericText(String defaultField, Analyzer analyzer) {
      super(defaultField, analyzer);

      // the query builder to be tested
      addQueryBuilder("GenericTextQuery", new GenericTextQueryBuilder(
        defaultField, analyzer, null, queryFactory));
    }
  }

  @Override
  protected CoreParser newCoreParser(String defaultField, Analyzer analyzer) {
    return new CoreParserGenericText(defaultField, analyzer);
  }

  @Override
  protected Query parse(String xmlFileName) throws ParserException, IOException {
    try (InputStream xmlStream = TestCoreParserGenericText.class.getResourceAsStream(xmlFileName)) {
      if (xmlStream == null) {
        return super.parse(xmlFileName);
      }
      Query result = coreParser().parse(xmlStream);
      return result;
    }
  }

  public void testGenericTextQueryXML() throws Exception {
    Query q = parse("GenericTextQuery.xml");
    assertTrue("Expecting a BoostQuery, but resulted in " + q.getClass(), q instanceof BoostQuery);
    Query nq = ((BoostQuery)q).getQuery();
    assertTrue("Expecting a nested PhraseQuery, but resulted in " + nq.getClass(), nq instanceof PhraseQuery);
    dumpResults("GenericTextQuery", q, 5);
  }

  public void testGenericTextQuerySingleTermXML() throws Exception {
    Query q = parse("GenericTextQuerySingleTerm.xml");
    assertTrue("Expecting a BoostQuery, but resulted in " + q.getClass(), q instanceof BoostQuery);
    Query nq = ((BoostQuery)q).getQuery();
    assertTrue("Expecting a nested TermQuery, but resulted in " + nq.getClass(), nq instanceof TermQuery);
    dumpResults("GenericTextQuery", q, 5);
  }

  public void testGenericTextQueryWithStopwordsXML() throws Exception {
    Query q = parse("GenericTextQueryStopwords.xml");
    assertTrue("Expecting a BoostQuery, but resulted in " + q.getClass(), q instanceof BoostQuery);
    Query nq = ((BoostQuery)q).getQuery();
    assertTrue("Expecting a nested PhraseQuery, but resulted in " + nq.getClass(), nq instanceof PhraseQuery);
    dumpResults("GenericTextQuery with stopwords", q, 5);
  }

  public void testGenericTextQueryWithAllStopwordsXML() throws Exception {
    Query q = parse("GenericTextQueryAllStopwords.xml");
    if (analyzer() instanceof StandardAnalyzer)
      assertTrue("Expecting a MatchAllDocsQuery, but resulted in " + q.getClass(), q instanceof MatchAllDocsQuery);
    dumpResults("GenericTextQuery with just stopwords", q, 5);
  }

  public void testGenericTextQueryWithNoTextXML() throws Exception {
    Query q = parse("GenericTextQueryEmpty.xml");
    assertTrue("Expecting a MatchAllDocsQuery, but resulted in " + q.getClass(), q instanceof MatchAllDocsQuery);
    dumpResults("GenericTextQuery with no text", q, 5);
  }

  public void testGenericTextQueryPhraseWildcardXML() throws Exception {
    Query q = parse("GenericTextQueryPhraseWildcard.xml");
    dumpResults("GenericTextQuery with a phrase wildcard", q, 5);
  }

  public void testGenericTextQueryTrailingWildcardXML() throws Exception {
    Query q = parse("GenericTextQueryTrailingWildcard.xml");
    dumpResults("GenericTextQuery with a trailing wildcard", q, 5);
  }

  public void testGenericTextQueryMultiWildcardXML() throws Exception {
    Query q = parse("GenericTextQueryMultiWildcard.xml");
    dumpResults("GenericTextQuery with multiple terms containing wildcards", q, 5);
  }

  public void testGenericTextQueryPhraseWildcard2XML() throws Exception {
    Query q = parse("GenericTextQueryPhraseWildcard2.xml");
    dumpResults("GenericTextQuery with a phrase wildcard", q, 5);
  }

  public void testGenericTextQueryTrailingWildcard2XML() throws Exception {
    Query q = parse("GenericTextQueryTrailingWildcard2.xml");
    dumpResults("GenericTextQuery with a trailing wildcard", q, 5);
  }

  public void testGenericTextQueryMultiWildcard2XML() throws Exception {
    Query q = parse("GenericTextQueryMultiWildcard2.xml");
    dumpResults("GenericTextQuery with multiple terms containing wildcards", q, 5);
  }

  public void testGenericTextQueryMultiClauseXML() throws Exception {
    Query q = parse("GenericTextQueryMultiClause.xml");
    dumpResults("GenericTextQuery. BooleanQuery containing multiple GenericTextQuery clauses with different boost factors", q, 5);
  }

  public void testGenericTextQueryMaxBooleanClausesWithPrefixQuery() throws ParserException, IOException {
    final int maxClauseCount = BooleanQuery.getMaxClauseCount();
    try {
      BooleanQuery.setMaxClauseCount(3);
      final Query q = parse("GenericTextQueryMaxBooleanClausesWithPrefixQuery.xml");
      dumpResults("GenericTextQueryMaxBooleanClausesWithPrefixQuery", q, 5);
    } finally {
      BooleanQuery.setMaxClauseCount(maxClauseCount);
    }
  }

}
