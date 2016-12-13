/*
 * Created on 25-Jan-2006
 */
package com.bloomberg.news.solr.search.xml;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.builders.SpanQueryBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrSpanQueryBuilder;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
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
 * Builder for {@link BooleanQuery}
 */
public class BooleanQueryBuilder extends SolrSpanQueryBuilder {

  public BooleanQueryBuilder(String defaultField, Analyzer analyzer,
                             SolrQueryRequest req,
                             SpanQueryBuilder spanFactory) {
    super(defaultField, analyzer, req, spanFactory);
  }

  /* (non-Javadoc)
    * @see org.apache.lucene.xmlparser.QueryObjectBuilder#process(org.w3c.dom.Element)
    */

  @Override
  public Query getQuery(Element e) throws ParserException {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.setDisableCoord(DOMUtils.getAttribute(e, "disableCoord", false));
    bq.setMinimumNumberShouldMatch(DOMUtils.getAttribute(e, "minimumNumberShouldMatch", 0));

    boolean matchAllDocsExists = false;
    boolean shouldOrMustExists = false;

    HashSet<BooleanClause> clauseDedupeSet = new HashSet<BooleanClause>();
    NodeList nl = e.getChildNodes();
    final int nlLen = nl.getLength();
    for (int i = 0; i < nlLen; i++) {
      Node node = nl.item(i);
      if (node.getNodeName().equals("Clause")) {
        Element clauseElem = (Element) node;
        BooleanClause.Occur occurs = getOccursValue(clauseElem);

        Element clauseQuery = DOMUtils.getFirstChildOrFail(clauseElem);
        Query q = queryFactory.getQuery(clauseQuery);
        if (q instanceof MatchAllDocsQuery) {
          matchAllDocsExists = true;
          continue;// we will add this MAD query later if necessary
        }
        else if ((occurs == BooleanClause.Occur.SHOULD) || (occurs == BooleanClause.Occur.MUST)){
          shouldOrMustExists = true;
        }
        BooleanClause bc = new BooleanClause(q, occurs);
        if (clauseDedupeSet.add(bc)){//dedupe check
          bq.add(bc);
        }
      }
    }

    // MatchAllDocsQuery needs to be added only if there is no other must or should clauses in the query.
    // At least we preserve the user's intention to execute the rest of the query, instead of flooding them with all the documents.
    if (matchAllDocsExists && !shouldOrMustExists) {
      bq.add(new BooleanClause(new MatchAllDocsQuery(), BooleanClause.Occur.MUST));
    }
    Query q = bq.build();

    if (((BooleanQuery)q).clauses().size() == 1)
      return ((BooleanQuery)q).clauses().get(0).getQuery();

    float boost = DOMUtils.getAttribute(e, "boost", 1.0f);
    if (boost != 1f) {
      q = new BoostQuery(q, boost);
    }

    return q;
  }

  @Override
  public SpanQuery getSpanQuery(Element e) throws ParserException {
    ArrayList<SpanQuery> ors = new ArrayList<>();
    ArrayList<SpanQuery> ands = new ArrayList<>();
    ArrayList<SpanQuery> nots = new ArrayList<>();

    HashSet<BooleanClause> clauseDedupeSet = new HashSet<BooleanClause>();
    NodeList nl = e.getChildNodes();
    final int nlLen = nl.getLength();
    for (int i = 0; i < nlLen; i++) {
      Node node = nl.item(i);
      if (node.getNodeName().equals("Clause")) {
        Element clauseElem = (Element) node;
        BooleanClause.Occur occurs = getOccursValue(clauseElem);

        Element clauseQuery = DOMUtils.getFirstChildOrFail(clauseElem);
        SpanQuery q = spanFactory.getSpanQuery(clauseQuery);

        // select the list to which we will add these options
        ArrayList<SpanQuery> chosenList = ors;
        if (occurs == BooleanClause.Occur.MUST) {
          chosenList = ands;
        } else if (occurs == BooleanClause.Occur.MUST_NOT) {
          chosenList = nots;
        }

        BooleanClause bc = new BooleanClause(q, occurs);
        if (clauseDedupeSet.add(bc)){//dedupe check
          chosenList.add(q);
        }
      }
    }

    SpanOrQuery orQuery = null;
    SpanQuery andQuery = null;
    SpanOrQuery notQuery = null;

    if (ors.size() > 0) {
      orQuery = new SpanOrQuery(ors.toArray(new SpanQuery[ors.size()]));
    }

    if (ands.size() > 1) {
      if (orQuery != null) {
        ands.add(orQuery);
      }
      andQuery = new SpanNearQuery(ands.toArray(new SpanQuery[ands.size()]), Integer.MAX_VALUE, false);
    } else if (ands.size() > 0) {
      andQuery = ands.get(0);
    }

    if (nots.size() > 0) {
      notQuery = new SpanOrQuery(nots.toArray(new SpanQuery[nots.size()]));

      if (andQuery != null) {
        return new SpanNotQuery(andQuery, notQuery);
      }
      else if (orQuery != null) {
        return new SpanNotQuery(orQuery, notQuery);
      }
      else {
        // TODO fix this
        return new SpanNotQuery(null, notQuery);
      }
    }

    return (andQuery != null ? andQuery : orQuery);
  }

  static BooleanClause.Occur getOccursValue(Element clauseElem) throws ParserException {
    String occs = clauseElem.getAttribute("occurs");
    if (occs == null || "should".equalsIgnoreCase(occs)) {
      return BooleanClause.Occur.SHOULD;
    } else if ("must".equalsIgnoreCase(occs)) {
      return BooleanClause.Occur.MUST;
    } else if ("mustNot".equalsIgnoreCase(occs)) {
      return BooleanClause.Occur.MUST_NOT;
    } else if ("filter".equals(occs)) {
      return BooleanClause.Occur.FILTER;
    }
    throw new ParserException("Invalid value for \"occurs\" attribute of clause:" + occs);
  }
}
