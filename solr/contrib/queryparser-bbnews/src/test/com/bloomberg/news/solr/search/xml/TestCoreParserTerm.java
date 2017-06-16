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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;
import java.io.InputStream;

public class TestCoreParserTerm extends TestCoreParser {

  private class CoreParserTerm extends CoreParser {
    final private TermFreqBuildersWrapper tfBuildersWrapper;
    CoreParserTerm(String defaultField, Analyzer analyzer) {
      super(defaultField, analyzer);

      // the builders to be tested
      this.tfBuildersWrapper = new TermFreqBuildersWrapper(defaultField, analyzer, null, this);
    }
  }

  @Override
  protected CoreParser newCoreParser(String defaultField, Analyzer analyzer) {
    final CoreParser coreParser = new CoreParserTerm(defaultField, analyzer);

    // some additional builders (if any) to help

    return coreParser;
  }

  @Override
  protected Query parse(String xmlFileName) throws ParserException, IOException {
    try (InputStream xmlStream = TestCoreParserTerm.class.getResourceAsStream(xmlFileName)) {
      if (xmlStream == null) {
        return super.parse(xmlFileName);
      }
      assertNotNull("Test XML file " + xmlFileName + " cannot be found", xmlStream);
      Query result = coreParser().parse(xmlStream);
      return result;
    }
  }

  public void testTermQueryStopwordXML() throws IOException {
    parseShouldFail("TermQueryStopwords.xml",
        "Empty term found. field:contents value:to a. Check the query analyzer configured on this field.");
  }
  
  public void testTermQueryMultipleTermsXML() throws IOException {
    parseShouldFail("TermQueryMultipleTerms.xml",
        "Multiple terms found. field:contents value:sumitomo come home. Check the query analyzer configured on this field.");
  }

  public void testTermsQueryShouldBeBooleanXML() throws ParserException, IOException {
    Query q = parse("TermsQuery.xml");
    assertTrue("Expecting a BooleanQuery, but resulted in " + q.getClass(), q instanceof BooleanQuery);
    dumpResults("TermsQuery", q, 5);
  }

  public void testTermsQueryWithTermElementXML() throws ParserException, IOException {
    Query q = parse("TermsQueryWithTermElement.xml");
    dumpResults("TermsQuery", q, 5);
  }
  
  public void testTermsQueryWithSingleTerm() throws ParserException, IOException {
    Query q = parse("TermsQuerySingleTerm.xml");
    assertTrue("Expecting a TermQuery, but resulted in " + q.getClass(), q instanceof TermQuery);
    dumpResults("TermsQueryWithSingleTerm", q, 5);
  }
  
  
  //term appears like single term but results in two terms when it runs through standard analyzer
  public void testTermsQueryWithStopwords() throws ParserException, IOException {
    Query q = parse("TermsQueryStopwords.xml");
    if (analyzer() instanceof StandardAnalyzer)
      assertTrue("Expecting a BooleanQuery, but resulted in " + q.getClass(), q instanceof BooleanQuery);
    dumpResults("TermsQueryWithStopwords", q, 5);
    }
  
  public void testTermsQueryEmpty() throws ParserException, IOException {
    Query q = parse("TermsQueryEmpty.xml");
    assertTrue("Expecting a MatchAllDocsQuery, but resulted in " + q.getClass(), q instanceof MatchAllDocsQuery);
    dumpResults("Empty TermsQuery", q, 5);
  }
  
  public void testTermsQueryWithOnlyStopwords() throws ParserException, IOException {
    Query q = parse("TermsQueryOnlyStopwords.xml");
    if (analyzer() instanceof StandardAnalyzer)
      assertTrue("Expecting a MatchAllDocsQuery, but resulted in " + q.getClass(), q instanceof MatchAllDocsQuery);
    dumpResults("TermsQuery with only stopwords", q, 5);
  }
  

  public void testTermsFilterXML() throws Exception {
    Query q = parse("TermsFilterQuery.xml");
    dumpResults("Terms Filter", q, 5);
  }
  
  public void testTermsFilterWithSingleTerm() throws Exception {
    Query q = parse("TermsFilterQueryWithSingleTerm.xml");
    dumpResults("TermsFilter With SingleTerm", q, 5);
  }
  
  public void testTermsFilterQueryWithStopword() throws Exception {
    Query q = parse("TermsFilterQueryStopwords.xml");
    dumpResults("TermsFilter with Stopword", q, 5);
  }
  
  public void testTermsFilterQueryWithOnlyStopword() throws Exception {
    Query q = parse("TermsFilterOnlyStopwords.xml");
    dumpResults("TermsFilter with all stop words", q, 5);
  }

}
