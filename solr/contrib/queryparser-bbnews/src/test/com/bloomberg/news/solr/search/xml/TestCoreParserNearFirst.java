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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class TestCoreParserNearFirst extends TestCoreParser {

  private class CoreParserNearFirstQuery extends CoreParser {
    CoreParserNearFirstQuery(String defaultField, Analyzer analyzer) {
      super(defaultField, analyzer);

      // the query builder to be tested
      addSpanQueryBuilder("NearFirstQuery", new NearFirstQueryBuilder(
        defaultField, analyzer, null, spanFactory));

      // some additional builders to help
      // (here only since requiring access to queryFactory)
      addSpanQueryBuilder("WildcardNearQuery", new WildcardNearQueryBuilder(
        defaultField, analyzer, null, spanFactory));
      addSpanQueryBuilder("TermQuery", TermFreqBuildersWrapper.newTermQueryBuilder(
          defaultField, analyzer, spanFactory));
     addSpanQueryBuilder("BooleanQuery", new BooleanQueryBuilder(
          defaultField, analyzer, null, spanFactory));
    }
  }

  @Override
  protected CoreParser newCoreParser(String defaultField, Analyzer analyzer) {
    final CoreParser coreParser = new CoreParserNearFirstQuery(defaultField, analyzer);

    return coreParser;
  }

  @Override
  protected Query parse(String xmlFileName) throws ParserException, IOException {
    try (InputStream xmlStream = TestCoreParserNearFirst.class.getResourceAsStream(xmlFileName)) {
      if (xmlStream == null) {
        return super.parse(xmlFileName);
      }
      Query result = coreParser().parse(xmlStream);
      return result;
    }
  }

  public void testNearFirstBooleanMustXml() throws IOException, ParserException {
    final Query q = parse("NearFirstBooleanMust.xml");
    dumpResults("testNearFirstBooleanMustXml", q, 50);
  }

  public void testNearFirstBooleanMust() throws IOException {
    ArrayList<SpanQuery> ands = new ArrayList<>();
    ands.add(new SpanTermQuery(new Term("contents", "upholds")));
    ands.add(new SpanTermQuery(new Term("contents", "building")));

    SpanNearQuery.Builder sqb = new SpanNearQuery.Builder("contents", false /* ordered */);
    sqb.setSlop(7);
    sqb.addClause(new SpanNearQuery(ands.toArray(new SpanQuery[ands.size()]), Integer.MAX_VALUE, false));
    sqb.addClause(new SpanTermQuery(new Term("contents", "akbar")));
    dumpResults("testNearFirstBooleanMust", sqb.build(), 5);
  }

}
