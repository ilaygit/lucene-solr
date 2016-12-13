package com.bloomberg.news.solr.search.xml;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.builders.SpanQueryBuilder;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.w3c.dom.Element;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrSpanQueryBuilder;

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

public class NearFirstQueryBuilder extends SolrSpanQueryBuilder {

  public NearFirstQueryBuilder(String defaultField, Analyzer analyzer,
      SolrQueryRequest req, SpanQueryBuilder spanFactory) {
      super(defaultField, analyzer, req, spanFactory);
  }

  @Override
  public SpanQuery getSpanQuery(Element e) throws ParserException {
    int end = DOMUtils.getAttribute(e, "end", 1);
    Element child = DOMUtils.getFirstChildElement(e);
    Query q = spanFactory.getQuery(child);
    if (q instanceof MatchAllDocsQuery)
      return null;

    SpanQuery sq = spanFactory.getSpanQuery(child);

    return new SpanFirstQuery(sq, end);
  }

  @Override
  public Query getQuery(Element e) throws ParserException {
    return null;
  }

}
