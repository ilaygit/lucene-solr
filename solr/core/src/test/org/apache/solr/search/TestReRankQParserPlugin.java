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
package org.apache.solr.search;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestReRankQParserPlugin extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-collapseqparser.xml", "schema11.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testReRankQParserPluginConstants() throws Exception {
    assertEquals(ReRankQParserPlugin.NAME, "rerank");

    assertEquals(ReRankQParserPlugin.RERANK_QUERY, "reRankQuery");

    assertEquals(ReRankQParserPlugin.RERANK_DOCS, "reRankDocs");
    assertEquals(ReRankQParserPlugin.RERANK_DOCS_DEFAULT, 200);

    assertEquals(ReRankQParserPlugin.RERANK_WEIGHT, "reRankWeight");
    assertEquals(ReRankQParserPlugin.RERANK_WEIGHT_DEFAULT, 2.0d, 0.0d);
  }

  @Test
  public void testReRankQueries() throws Exception {

    assertU(delQ("*:*"));
    assertU(commit());

    String[] doc = {"id","1", "term_s", "YYYY", "group_s", "group1", "test_ti", "5", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc));
    assertU(commit());
    String[] doc1 = {"id","2", "term_s","YYYY", "group_s", "group1", "test_ti", "50", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc1));

    String[] doc2 = {"id","3", "term_s", "YYYY", "test_ti", "5000", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc2));
    assertU(commit());
    String[] doc3 = {"id","4", "term_s", "YYYY", "test_ti", "500", "test_tl", "1000", "test_tf", "2000"};
    assertU(adoc(doc3));

    String[] doc4 = {"id","5", "term_s", "YYYY", "group_s", "group2", "test_ti", "4", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc4));
    assertU(commit());
    String[] doc5 = {"id","6", "term_s","YYYY", "group_s", "group2", "test_ti", "10", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc5));
    assertU(commit());




    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("rq", "{!"+ReRankQParserPlugin.NAME+" "+ReRankQParserPlugin.RERANK_QUERY+"=$rqq "+ReRankQParserPlugin.RERANK_DOCS+"=200}");
    params.add("q", "term_s:YYYY");
    params.add("rqq", "{!edismax bf=$bff}*:*");
    params.add("bff", "field(test_ti)");
    params.add("start", "0");
    params.add("rows", "6");
    params.add("df", "text");
    assertQ(req(params), "*[count(//doc)=6]",
        "//result/doc[1]/float[@name='id'][.='3.0']",
        "//result/doc[2]/float[@name='id'][.='4.0']",
        "//result/doc[3]/float[@name='id'][.='2.0']",
        "//result/doc[4]/float[@name='id'][.='6.0']",
        "//result/doc[5]/float[@name='id'][.='1.0']",
        "//result/doc[6]/float[@name='id'][.='5.0']"
    );
  }

  @Test
  public void testOverRank() throws Exception {

    assertU(delQ("*:*"));
    assertU(commit());

    //Test the scenario that where we rank more documents then we return.

    String[] doc = {"id","1", "term_s", "YYYY", "group_s", "group1", "test_ti", "5", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc));
    String[] doc1 = {"id","2", "term_s","YYYY", "group_s", "group1", "test_ti", "50", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc1));

    String[] doc2 = {"id","3", "term_s", "YYYY", "test_ti", "5000", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc2));
    String[] doc3 = {"id","4", "term_s", "YYYY", "test_ti", "500", "test_tl", "1000", "test_tf", "2000"};
    assertU(adoc(doc3));


    String[] doc4 = {"id","5", "term_s", "YYYY", "group_s", "group2", "test_ti", "4", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc4));

    String[] doc5 = {"id","6", "term_s","YYYY", "group_s", "group2", "test_ti", "10", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc5));

    String[] doc6 = {"id","7", "term_s", "YYYY", "group_s", "group1", "test_ti", "5", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc6));


    String[] doc7 = {"id","8", "term_s","YYYY", "group_s", "group1", "test_ti", "50", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc7));

    String[] doc8 = {"id","9", "term_s", "YYYY", "test_ti", "5000", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc8));
    String[] doc9 = {"id","10", "term_s", "YYYY", "test_ti", "500", "test_tl", "1000", "test_tf", "2000"};
    assertU(adoc(doc9));

    String[] doc10 = {"id","11", "term_s", "YYYY", "group_s", "group2", "test_ti", "4", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc10));
    assertU(commit());

 }

  @Test
  public void testRerankQueryParsingShouldFailWithoutMandatoryReRankQueryParameter() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());

    String[] doc = {"id", "1", "term_s", "YYYY", "group_s", "group1", "test_ti", "5", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc));
    assertU(commit());
    String[] doc1 = {"id", "2", "term_s", "YYYY", "group_s", "group1", "test_ti", "50", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc1));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();

    params.add("rq", "{!"+ReRankQParserPlugin.NAME+" "+ReRankQParserPlugin.RERANK_QUERY+"=$rqq "+ReRankQParserPlugin.RERANK_DOCS+"=200}");
    params.add("q", "term_s:YYYY");
    params.add("start", "0");
    params.add("rows", "2");

    try {
      h.query(req(params));
      fail("A syntax error should be thrown when "+ReRankQParserPlugin.RERANK_QUERY+" parameter is not specified");
    } catch (SolrException e) {
      assertTrue(e.code() == SolrException.ErrorCode.BAD_REQUEST.code);
    }
  }

  @Test
  public void testRerankQueryAndGroupingRerankGroups() throws Exception {
    assertU(delQ("*:*"));
    assertU(commit());
    String[] doc1 = {"id", "1", "term_s", "YYYY", "group_s", "group1", "test_ti", "5", "test_tl", "9", "test_tf",
        "2000"};
    assertU(adoc(doc1));
    assertU(commit());
    String[] doc2 = {"id", "2", "term_s", "YYYY", "group_s", "group1", "test_ti", "50", "test_tl", "8", "test_tf",
        "200"};
    assertU(adoc(doc2));
    assertU(commit());
    String[] doc3 = {"id", "3", "term_s", "YYYY", "group_s", "group2", "test_ti", "100", "test_tl", "2", "test_tf",
        "2000"};
    assertU(adoc(doc3));
    assertU(commit());
    String[] doc4 = {"id", "4", "term_s", "YYYY", "group_s", "group2", "test_ti", "74", "test_tl", "3", "test_tf",
        "200"};
    assertU(adoc(doc4));
    String[] doc5 = {"id", "5", "term_s", "YYYY", "group_s", "group3", "test_ti", "1000", "test_tl", "1", "test_tf",
    "200"};
    assertU(adoc(doc5));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();

    // first query will sort the documents on the value of test_tl
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "{!func }field(test_tl)");

    params.add("start", "0");
    params.add("rows", "6");
    params.add("fl", "id,score,[explain]");

    assertQ(req(params), "*[count(//doc)=5]",
        "//result/doc[1]/float[@name='id'][.='1.0']", // group1
        "//result/doc[2]/float[@name='id'][.='2.0']", // group1
        "//result/doc[3]/float[@name='id'][.='4.0']", // group2
        "//result/doc[4]/float[@name='id'][.='3.0']", // group2
        "//result/doc[5]/float[@name='id'][.='5.0']"); // group3

    // add reranking
    params.add("rq", "{!" + ReRankQParserPlugin.NAME + " " + ReRankQParserPlugin.RERANK_QUERY + "=$rqq "
      + ReRankQParserPlugin.RERANK_DOCS + "=200}");
    //rank query, rerank documents on the value of test_ti
    params.add("rqq", "{!func }field(test_ti)");

    assertQ(req(params), "*[count(//doc)=5]",
        "//result/doc[1]/float[@name='id'][.='5.0']", // group3
        "//result/doc[2]/float[@name='id'][.='3.0']", // group2
        "//result/doc[3]/float[@name='id'][.='4.0']", // group2
        "//result/doc[4]/float[@name='id'][.='2.0']", // group1
        "//result/doc[5]/float[@name='id'][.='1.0']");// group1

    // now grouping and reranking should rescore the documents inside the groups and then
    // reorder the groups if the scores changed:
    // so:
    //
    // the result should be group3[doc5], group2[doc3], group1[doc2]

    params.add("group", "true");
    params.add("group.field", "group_s");

    assertQ(req(params),
        "//arr/lst[1]/result/doc/float[@name='id'][.='5.0']",
        "//arr/lst[2]/result/doc/float[@name='id'][.='3.0']",
        "//arr/lst[3]/result/doc/float[@name='id'][.='2.0']"
    );

    // Rerank top 3 groups, should not make any difference now:
    params.remove("rq");
    params.add("rq", "{!" + ReRankQParserPlugin.NAME + " " + ReRankQParserPlugin.RERANK_QUERY + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS + "=3}");

    assertQ(req(params),
            "//arr/lst[1]/result/doc/float[@name='id'][.='5.0']",
            "//arr/lst[2]/result/doc/float[@name='id'][.='3.0']",
            "//arr/lst[3]/result/doc/float[@name='id'][.='2.0']"
    );

    // .. But if we rerank only the first 2 groups, results must be different
    // Rerank top 3 groups, should not make any difference now:
    params.remove("rq");
    params.add("rq", "{!" + ReRankQParserPlugin.NAME + " " + ReRankQParserPlugin.RERANK_QUERY + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS + "=2}");
    assertQ(req(params),
            "//arr/lst[1]/result/doc/float[@name='id'][.='3.0']",
            "//arr/lst[2]/result/doc/float[@name='id'][.='2.0']");

  }
}
