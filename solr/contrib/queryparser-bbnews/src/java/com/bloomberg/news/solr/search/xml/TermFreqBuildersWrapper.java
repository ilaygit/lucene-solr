package com.bloomberg.news.solr.search.xml;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.xml.CoreParser;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.queryparser.xml.builders.SpanQueryBuilder;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrSpanQueryBuilder;
import org.w3c.dom.Element;
import com.bloomberg.news.lucene.queryparser.xml.TermBuilder;
import com.bloomberg.news.lucene.queryparser.xml.builders.TermQueryBuilder;
import com.bloomberg.news.lucene.queryparser.xml.builders.TermsQueryBuilder;

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

/**
 * Utility class to facilitate configuration of Term[s][Freq]Query builders.
 */
public class TermFreqBuildersWrapper extends SolrSpanQueryBuilder {

  static SpanQueryBuilder newTermQueryBuilder(String defaultField, Analyzer analyzer,
      QueryBuilder queryFactory) {
    return new TermQueryBuilder(new TermBuilder(analyzer));
  }

  static SpanQueryBuilder newTermsQueryBuilder(String defaultField, Analyzer analyzer,
      QueryBuilder queryFactory) {
    return new TermsQueryBuilder(new TermBuilder(analyzer));
  }

  public TermFreqBuildersWrapper(String defaultField, Analyzer analyzer,
      SolrQueryRequest req,
      SpanQueryBuilder spanFactory) {
    super(defaultField, analyzer, req, spanFactory);

    final CoreParser coreParser = (CoreParser)queryFactory;

    final TermBuilder termBuilder = new TermBuilder(analyzer);

    {
      SpanQueryBuilder termQueryBuilder = new TermQueryBuilder(termBuilder);
      coreParser.addSpanQueryBuilder("TermQuery", termQueryBuilder);
      coreParser.addQueryBuilder("TermFreqQuery", new TermFreqBuilder(termQueryBuilder));
    }
    {
      SpanQueryBuilder termsQueryBuilder = new TermsQueryBuilder(termBuilder);
      coreParser.addSpanQueryBuilder("TermsQuery", termsQueryBuilder);
      coreParser.addQueryBuilder("TermsFreqQuery", new TermFreqBuilder(termsQueryBuilder));
    }
  }

  @Override
  public Query getQuery(Element e) throws ParserException {
    throw new ParserException("TermFreqBuildersWrapper.getQuery is unsupported");
  }

  @Override
  public SpanQuery getSpanQuery(Element e) throws ParserException {
    throw new ParserException("TermFreqBuildersWrapper.getSpanQuery is unsupported");
  }

}
