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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrQueryBuilder;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Builder for {@link DisjunctionMaxQuery}
 */
public class DisjunctionMaxQueryBuilder extends SolrQueryBuilder {

  public DisjunctionMaxQueryBuilder(String defaultField, Analyzer analyzer,
      SolrQueryRequest req, QueryBuilder queryFactory) {
    super(defaultField, analyzer, req, queryFactory);
  }

  /* (non-Javadoc)
    * @see org.apache.lucene.xmlparser.QueryObjectBuilder#process(org.w3c.dom.Element)
    */

  @Override
  public Query getQuery(Element e) throws ParserException {
    float tieBreaker = DOMUtils.getAttribute(e, "tieBreaker", 0.0f);

    boolean matchAllDocsExists = false;
    boolean anyOtherQueryExists = false;
    NodeList nl = e.getChildNodes();
    final int nlLen = nl.getLength();
    Collection<Query> queries = new ArrayList<Query>(nlLen);
    for (int i = 0; i < nlLen; i++) {
      Node node = nl.item(i);
      if (node instanceof Element) { // all elements are disjuncts.
        Element queryElem = (Element) node;
        Query q = queryFactory.getQuery(queryElem);
        if (q instanceof MatchAllDocsQuery) {
          matchAllDocsExists = true;
          continue;// we will add this MAD query later if necessary
        }
        else {
          anyOtherQueryExists = true;
        }
        queries.add(q);
      }
    }

    Query q = new DisjunctionMaxQuery(queries, tieBreaker);
    float boost = DOMUtils.getAttribute(e, "boost", 1.0f);
    if (boost != 1f) {
      q = new BoostQuery(q, boost);
    }

    //MatchallDocs query needs to be added only if there is no other queries inside the DisjunctionMaxQuery.
    //At least we preserve the users intention to execute the rest of the query. instead of flooding him with all the documents.
    if (matchAllDocsExists && !anyOtherQueryExists)
      return new MatchAllDocsQuery();
    else
      return q;
  }
}
