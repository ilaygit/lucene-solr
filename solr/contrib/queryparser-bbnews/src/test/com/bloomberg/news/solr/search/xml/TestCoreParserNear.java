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
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.xml.CoreParser;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.TestCoreParser;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class TestCoreParserNear extends TestCoreParser {

  private class CoreParserNearQuery extends CoreParser {
    CoreParserNearQuery(String defaultField, Analyzer analyzer) {
      super(defaultField, analyzer);

      // the query builder to be tested
      addSpanQueryBuilder("NearQuery", new NearQueryBuilder(
        defaultField, analyzer, null, spanFactory));

      // some additional builders to help
      // (here only since requiring access to queryFactory)
      addSpanQueryBuilder("BooleanQuery", new BooleanQueryBuilder(
          defaultField, analyzer, null, spanFactory));
      addSpanQueryBuilder("TermQuery", TermFreqBuildersWrapper.newTermQueryBuilder(
          defaultField, analyzer, spanFactory));
      addSpanQueryBuilder("WildcardNearQuery", new WildcardNearQueryBuilder(
        defaultField, analyzer, null, spanFactory));
    }
  }

  @Override
  protected CoreParser newCoreParser(String defaultField, Analyzer analyzer) {
    final CoreParser coreParser = new CoreParserNearQuery(defaultField, analyzer);

    return coreParser;
  }

  @Override
  protected Query parse(String xmlFileName) throws ParserException, IOException {
    try (InputStream xmlStream = TestCoreParserNear.class.getResourceAsStream(xmlFileName)) {
      if (xmlStream == null) {
        return super.parse(xmlFileName);
      }
      Query result = coreParser().parse(xmlStream);
      return result;
    }
  }

  public void testNearBooleanNear() throws IOException, ParserException {
    final Query q = parse("NearBooleanNear.xml");
    dumpResults("testNearBooleanNear", q, 5);
  }

  //working version of (A OR B) N/5 C
  public void testNearBoolean() throws IOException {
    ArrayList<SpanQuery> ors = new ArrayList<SpanQuery>();

    ors.add(new SpanTermQuery(new Term("contents", "iranian")));
    ors.add(new SpanTermQuery(new Term("contents", "north")));

    SpanNearQuery.Builder sqb = new SpanNearQuery.Builder("contents", false /* ordered */);
    sqb.setSlop(5);
    sqb.addClause(new SpanOrQuery(ors.toArray(new SpanQuery[ors.size()])));
    sqb.addClause(new SpanTermQuery(new Term("contents", "akbar")));
    dumpResults("testNearBoolean", sqb.build(), 5);
  }

  public void testNearTermQuery() throws ParserException, IOException {
    SpanNearQuery.Builder sqb = new SpanNearQuery.Builder("contents", true /* ordered */);
    sqb.setSlop(1);
    sqb.addClause(new SpanTermQuery(new Term("contents", "keihanshin")));
    sqb.addClause(new SpanTermQuery(new Term("contents", "real")));
    
    dumpResults("NearPrefixQuery", sqb.build(), 5);
  }

  public void testPrefixedNearQuery() throws ParserException, IOException {
    SpanNearQuery.Builder sqb = new SpanNearQuery.Builder("contents", true /* ordered */);
    sqb.setSlop(1);

    {
      SpanQuery clause = new SpanMultiTermQueryWrapper<PrefixQuery>(new PrefixQuery(new Term("contents", "keihanshi")));
      ((SpanMultiTermQueryWrapper<PrefixQuery>)clause).setRewriteMethod(SpanMultiTermQueryWrapper.SCORING_SPAN_QUERY_REWRITE);
      sqb.addClause(clause);
    }

    {
      SpanQuery clause = new SpanMultiTermQueryWrapper<PrefixQuery>(new PrefixQuery(new Term("contents", "rea")));
      ((SpanMultiTermQueryWrapper<PrefixQuery>)clause).setRewriteMethod(SpanMultiTermQueryWrapper.SCORING_SPAN_QUERY_REWRITE);
      sqb.addClause(clause);
    }

    dumpResults("NearPrefixQuery", sqb.build(), 5);
  }

}
