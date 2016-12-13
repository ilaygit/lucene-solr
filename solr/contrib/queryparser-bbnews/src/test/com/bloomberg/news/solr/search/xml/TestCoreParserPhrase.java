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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.PhraseQuery;

import java.io.IOException;
import java.io.InputStream;

public class TestCoreParserPhrase extends TestCoreParser {

  private class CoreParserPhraseQuery extends CoreParser {
    CoreParserPhraseQuery(String defaultField, Analyzer analyzer) {
      super(defaultField, analyzer);

      // the query builder to be tested
      addQueryBuilder("PhraseQuery", new PhraseQueryBuilder(
        defaultField, analyzer, null, queryFactory));
    }
  }

  @Override
  protected CoreParser newCoreParser(String defaultField, Analyzer analyzer) {
    return new CoreParserPhraseQuery(defaultField, analyzer);
  }

  @Override
  protected Query parse(String xmlFileName) throws ParserException, IOException {
    try (InputStream xmlStream = TestCoreParserPhrase.class.getResourceAsStream(xmlFileName)) {
      if (xmlStream == null) {
        return super.parse(xmlFileName);
      }
      assertNotNull("Test XML file " + xmlFileName + " cannot be found", xmlStream);
      Query result = coreParser().parse(xmlStream);
      return result;
    }
  }

  public void testPhraseQueryXML() throws Exception {
    Query q = parse("PhraseQuery.xml");
    assertTrue("Expecting a BoostQuery, but resulted in " + q.getClass(), q instanceof BoostQuery);
    Query nq = ((BoostQuery)q).getQuery();
    assertTrue("Expecting a nested PhraseQuery, but resulted in " + nq.getClass(), nq instanceof PhraseQuery);
    dumpResults("PhraseQuery", q, 5);
  }

  public void testPhraseQueryXMLWithStopwordsXML() throws Exception {
    if (analyzer() instanceof StandardAnalyzer) {
      parseShouldFail("PhraseQueryStopwords.xml",
          "Empty phrase query generated for field:contents, phrase:and to a");
    }
  }

  public void testPhraseQueryXMLWithNoTextXML() throws Exception {
    parseShouldFail("PhraseQueryEmpty.xml",
        "PhraseQuery has no text");
  }

}
