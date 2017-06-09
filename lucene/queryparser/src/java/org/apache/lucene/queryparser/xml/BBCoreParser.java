package org.apache.lucene.queryparser.xml;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.xml.builders.*;

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
 * CoreParser + Bloomberg (News) specific custom builders
 */
public class BBCoreParser extends CoreParser {

  final private TermFreqBuildersWrapper tfBuildersWrapper;

  /**
   * Construct an XML parser that uses a single instance QueryParser for handling
   * UserQuery tags - all parse operations are synchronized on this parser
   *
   * @param parser A QueryParser which will be synchronized on during parse calls.
   */
  public BBCoreParser(Analyzer analyzer, QueryParser parser) {
    this(null, analyzer, parser);
  }

  /**
   * Constructs an XML parser that creates a QueryParser for each UserQuery request.
   *
   * @param defaultField The default field name used by QueryParsers constructed for UserQuery tags
   */
  public BBCoreParser(String defaultField, Analyzer analyzer) {
    this(defaultField, analyzer, null);
  }

  protected BBCoreParser(String defaultField, Analyzer analyzer, QueryParser parser) {
    super(defaultField, analyzer, parser);

    this.tfBuildersWrapper = new TermFreqBuildersWrapper(defaultField, analyzer, this);
    
    queryFactory.addBuilder("BooleanQuery", new BBBooleanQueryBuilder(queryFactory));
    filterFactory.addBuilder("BooleanFilter", new BBBooleanFilterBuilder(filterFactory));

    //GenericTextQuery is a error tolerant version of PhraseQuery
    queryFactory.addBuilder("GenericTextQuery", new GenericTextQueryBuilder(analyzer));
    
    queryFactory.addBuilder("NearQuery", new NearQueryBuilder(queryFactory));
    queryFactory.addBuilder("NearFirstQuery", new NearFirstQueryBuilder(queryFactory));
    queryFactory.addBuilder("WildcardNearQuery", new WildcardNearQueryBuilder(analyzer));

  }
}
