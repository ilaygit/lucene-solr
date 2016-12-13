package com.bloomberg.news.solr.search.xml;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrSpanQueryBuilder;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.builders.SpanQueryBuilder;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.w3c.dom.Element;

import com.bloomberg.news.lucene.queryparser.xml.builders.WildcardNearQueryParser;

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

/* Builder for keyword phrases specially for wildcarded multi term queries.
 * Text phrases can be thrown into this builder to get tokenized to form OrderedNearQuery of sub queries of individual tokens.
 * Currently this can result in WildcardQuery,PrefixQuery and TermQuery as its sub queries.
 */

public class WildcardNearQueryBuilder extends SolrSpanQueryBuilder {

  protected Analyzer analyzer;

  public WildcardNearQueryBuilder(String defaultField, Analyzer analyzer,
      SolrQueryRequest req, SpanQueryBuilder spanFactory) {
      super(defaultField, analyzer, req, spanFactory);
      this.analyzer = analyzer;
  }

  @Override
  public Query getQuery(Element e) throws ParserException {
    String field = DOMUtils.getAttributeWithInheritanceOrFail(e, "fieldName");
    String text = DOMUtils.getText(e);
    boolean ignoreWildcard = DOMUtils.getAttribute(e, "ignoreWC", false);
    WildcardNearQueryParser p = new WildcardNearQueryParser(field, analyzer);
    return p.parse(text,ignoreWildcard);
  }

  @Override
  public SpanQuery getSpanQuery(Element e) throws ParserException {
    Query q = getQuery(e);

    if (q instanceof SpanQuery) {
      return (SpanQuery)q;
    }

    // TODO fix this
    return null;
  }
}
