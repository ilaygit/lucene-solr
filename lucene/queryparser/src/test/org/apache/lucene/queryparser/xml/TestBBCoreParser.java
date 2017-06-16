package org.apache.lucene.queryparser.xml;

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
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchAllDocsFilter;
import org.w3c.dom.Element;

// Deprecated
public class TestBBCoreParser extends TestCoreParser {

  private static String ANALYSER_PARAM     = "tests.TestParser.analyser";
  private static String DEFAULT_ANALYSER   = "mock";
  private static String STANDARD_ANALYSER  = "standard";
  private static String KEYWORD_ANALYSER   = "keyword";
 
  @Override
  protected Analyzer newAnalyzer() {
    final Analyzer analyzer;
    String analyserToUse = System.getProperty(ANALYSER_PARAM, DEFAULT_ANALYSER);
    if (analyserToUse.equals(STANDARD_ANALYSER))
    {
      analyzer = new StandardAnalyzer(TEST_VERSION_CURRENT);
    }
    else if (analyserToUse.equals(KEYWORD_ANALYSER))
    {
      analyzer = new KeywordAnalyzer();
    }
    else
    {
      assertEquals(DEFAULT_ANALYSER, analyserToUse);
      // TODO: rewrite test (this needs to set QueryParser.enablePositionIncrements, too, for work with CURRENT):
      analyzer = super.newAnalyzer();
    }
    return analyzer;
  }

  @Override
  protected CoreParser newCoreParser(String defaultField, Analyzer analyzer) {
    final CoreParser bbCoreParser = new BBCoreParser(defaultField, analyzer);
    
    //MatchAllDocsFilter is not yet in side the builderFactory
    //Remove this when we have MatchAllDocsFilter within CorePlusExtensionsParser
    bbCoreParser.filterFactory.addBuilder("MatchAllDocsFilter", new FilterBuilder() {
      
      @Override
      public Filter getFilter(Element e) throws ParserException {
        return new MatchAllDocsFilter();
      }
    });

    return bbCoreParser;
  }

}
