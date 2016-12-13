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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.apache.lucene.analysis.ja.JapaneseBaseFormFilter;
import org.apache.lucene.analysis.ja.JapaneseKatakanaStemFilter;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.LuceneTestCase;

import com.bloomberg.news.lucene.analysis.ja.JapaneseNumberFilter;
import com.bloomberg.news.lucene.analysis.standard.FinancialStandardTokenizer;
import com.bloomberg.news.lucene.queryparser.xml.builders.WildcardNearQueryParser;

public class TestWildParser extends LuceneTestCase {
  private final class TestAnalyzer extends Analyzer {
    // This is an approximation of an Analyzer that is actually built in Solr
    // using a config file.
    public TestAnalyzer() {
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
      final Tokenizer src = new FinancialStandardTokenizer();
      TokenStream tok = new ICUFoldingFilter(src);
      tok = new LowerCaseFilter(tok);
      return new TokenStreamComponents(src, tok);
    }
  }

  private final class TestEnglishStemAnalyzer extends Analyzer {
    // Let's make sure that we do the right thing if our analyzer does stemming.
    public TestEnglishStemAnalyzer() {
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
      final Tokenizer src = new FinancialStandardTokenizer();
      TokenStream tok = new ICUFoldingFilter(src);
      tok = new LowerCaseFilter(tok);
      tok = new PorterStemFilter(tok);
      return new TokenStreamComponents(src, tok);
    }
  }

  private final class TestJapaneseAnalyzerDiscardPunct extends Analyzer {
    // This is an approximation of an Analyzer that is actually built in Solr
    // using a config file.
    public TestJapaneseAnalyzerDiscardPunct() {
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
      final JapaneseTokenizer src = new JapaneseTokenizer(null, true, JapaneseTokenizer.Mode.SEARCH);
      TokenStream tok = new JapaneseNumberFilter(src);
      tok = new JapaneseBaseFormFilter(tok);
      tok = new CJKWidthFilter(tok);
      tok = new JapaneseKatakanaStemFilter(tok);
      tok = new LowerCaseFilter(tok);
      return new TokenStreamComponents(src, tok);
    }
  }

  private final class JapaneseAnalyzerDontDiscardPunct extends Analyzer {
    // This is an approximation of an Analyzer that is actually built in Solr
    // using a config file.
    public JapaneseAnalyzerDontDiscardPunct() {
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
      final JapaneseTokenizer src = new JapaneseTokenizer(null, false, JapaneseTokenizer.Mode.SEARCH);
      TokenStream tok = new JapaneseNumberFilter(src);
      tok = new JapaneseBaseFormFilter(tok);
      tok = new CJKWidthFilter(tok);
      tok = new JapaneseKatakanaStemFilter(tok);
      tok = new LowerCaseFilter(tok);
      return new TokenStreamComponents(src, tok);
    }
  }

  private static void checkQuery(WildcardNearQueryParser p, String query, String result)
      throws Exception {
    checkQuery(p, query, result, false);
  }

  private static void checkQuery(WildcardNearQueryParser p, String query, String result,
      boolean ignoreWC) throws Exception {
    Query q = p.parse(query, ignoreWC);
    String qs = q.toString();
    if (VERBOSE) {
      System.out.println("Check if equal '" + qs + "' == '" + result + "'");
    }
    assertEquals(qs, result);
  }

  private static void checkQueryEqual(WildcardNearQueryParser p, String query, Query expected_q,
      boolean ignoreWC) throws Exception {
    Query q = p.parse(query, ignoreWC);
    String qs = q.toString();
    if (VERBOSE) {
      System.out.println("Check if equal '" + qs.toString() + "' == '" + expected_q.toString() + "'");
    }
    assertTrue(q.equals(expected_q));
  }

  private static SpanMultiTermQueryWrapper<MultiTermQuery> fixRewrite(MultiTermQuery q) {
    q.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE);
    SpanMultiTermQueryWrapper<MultiTermQuery> sq = new SpanMultiTermQueryWrapper<MultiTermQuery>(q);
    sq.setRewriteMethod(SpanMultiTermQueryWrapper.SCORING_SPAN_QUERY_REWRITE);
    return sq;
  }

  public void testWildcardNearQueryParser() throws Exception {
    String field = "headline";
    boolean ignoreWC = false;
    WildcardNearQueryParser p = new WildcardNearQueryParser(field, new TestAnalyzer());

    checkQueryEqual(p, "*", new MatchAllDocsQuery(), ignoreWC);
    checkQueryEqual(p, "?", fixRewrite(new WildcardQuery(new Term(field, "?"))), ignoreWC);

    // Made up:
    SpanNearQuery.Builder sqb = new SpanNearQuery.Builder(field, true /* ordered */);
    sqb.setSlop(0);
    sqb.addClause(new SpanTermQuery(new Term(field, "london")));
    sqb.addClause(new SpanTermQuery(new Term(field, "city")));
    sqb.addClause(new SpanTermQuery(new Term(field, "airport")));
    Query q = sqb.build();
    checkQueryEqual(p, "London-City-Airport", q, ignoreWC);
    checkQueryEqual(p, "London City Airport", q, ignoreWC);

    sqb = new SpanNearQuery.Builder(field, true /* ordered */);
    sqb.setSlop(0);
    sqb.addClause(new SpanTermQuery(new Term(field, "london")));
    sqb.addClause(fixRewrite(new PrefixQuery(new Term(field, "cit"))));
    sqb.addClause(new SpanTermQuery(new Term(field, "airport")));
    q = sqb.build();
    checkQueryEqual(p, "London-Cit*-Airport", q, ignoreWC);

    sqb = new SpanNearQuery.Builder(field, true /* ordered */);
    sqb.setSlop(0);
    sqb.addClause(new SpanTermQuery(new Term(field, "london")));
    sqb.addClause(fixRewrite(new WildcardQuery(new Term(field, "*ty"))));
    sqb.addClause(new SpanTermQuery(new Term(field, "airport")));
    q = sqb.build();
    checkQueryEqual(p, "London-*ty-Airport", q, ignoreWC);

    sqb = new SpanNearQuery.Builder(field, true /* ordered */);
    sqb.setSlop(1);
    sqb.addClause(new SpanTermQuery(new Term(field, "london")));
    sqb.addClause(new SpanTermQuery(new Term(field, "airport")));
    q = sqb.build();
    checkQueryEqual(p, "London-*-Airport", q, ignoreWC);
    checkQueryEqual(p, "London-* Airport", q, ignoreWC);
    checkQueryEqual(p, "London *-Airport", q, ignoreWC);
    checkQueryEqual(p, "London * Airport", q, ignoreWC);

    sqb = new SpanNearQuery.Builder(field, true /* ordered */);
    sqb.setSlop(0);
    sqb.addClause(new SpanTermQuery(new Term(field, "london")));
    sqb.addClause(fixRewrite(new WildcardQuery(new Term(field, "?"))));
    sqb.addClause(new SpanTermQuery(new Term(field, "airport")));
    q = sqb.build();
    checkQueryEqual(p, "London ? Airport", q, ignoreWC);

    sqb = new SpanNearQuery.Builder(field, true /* ordered */);
    sqb.setSlop(0);
    sqb.addClause(fixRewrite(new WildcardQuery(new Term(field, "??"))));
    sqb.addClause(new SpanTermQuery(new Term(field, "london")));
    sqb.addClause(fixRewrite(new WildcardQuery(new Term(field, "??"))));
    q = sqb.build();
    checkQueryEqual(p, "?? London ??", q, ignoreWC);

    checkQuery(p, "London City * Airport", "spanNear([spanNear([headline:london, headline:city], 0, true), headline:airport], 1, true)");

    sqb = new SpanNearQuery.Builder(field, true /* ordered */);
    sqb.setSlop(1);
    sqb.addClause(fixRewrite(new PrefixQuery(new Term(field, "lon"))));
    sqb.addClause(fixRewrite(new WildcardQuery(new Term(field, "*port"))));
    q = sqb.build();
    checkQueryEqual(p, "Lon*-* *port", q, ignoreWC);

    checkQuery(p, "Lon*-* Airport", "spanNear([SpanMultiTermQueryWrapper(headline:lon*), headline:airport], 1, true)");
    checkQuery(p, "Lon*-City Airport", "spanNear([SpanMultiTermQueryWrapper(headline:lon*), headline:city, headline:airport], 0, true)");

    checkQueryEqual(p, "Lon*", fixRewrite(new PrefixQuery(new Term(field, "lon"))), ignoreWC);

    checkQuery(p, "trick-or-treat", "spanNear([headline:trick, headline:or, headline:treat], 0, true)");
    checkQuery(p, "trick-??-treat", "spanNear([headline:trick, SpanMultiTermQueryWrapper(headline:??), headline:treat], 0, true)");
    checkQuery(p, "trick-?*?-treat", "spanNear([headline:trick, SpanMultiTermQueryWrapper(headline:?*?), headline:treat], 0, true)");
    checkQuery(p, "trick-**-treat", "spanNear([headline:trick, headline:treat], 1, true)");
    checkQuery(p, "trick-??-?????", "spanNear([headline:trick, SpanMultiTermQueryWrapper(headline:??), SpanMultiTermQueryWrapper(headline:?????)], 0, true)");
    checkQuery(p, "trick-*-?????", "spanNear([headline:trick, SpanMultiTermQueryWrapper(headline:?????)], 1, true)");
    checkQuery(p, "trick-??-* candy", "spanNear([spanNear([headline:trick, SpanMultiTermQueryWrapper(headline:??)], 0, true), headline:candy], 1, true)");
    checkQuery(p, "trick-*-treat", "spanNear([headline:trick, headline:treat], 1, true)");
    checkQuery(p, "trick-*-*-treat", "spanNear([headline:trick, headline:treat], 2, true)");
    checkQuery(p, "trick * * treat", "spanNear([headline:trick, headline:treat], 2, true)");
    checkQuery(p, "i love trick-*-treat candy", "spanNear([spanNear([headline:i, headline:love, headline:trick], 0, true), spanNear([headline:treat, headline:candy], 0, true)], 1, true)");

    checkQuery(p, "slow up?-side down?", "spanNear([headline:slow, SpanMultiTermQueryWrapper(headline:up?), headline:side, SpanMultiTermQueryWrapper(headline:down?)], 0, true)");
    checkQuery(p, "slow up??-side down??", "spanNear([headline:slow, SpanMultiTermQueryWrapper(headline:up??), headline:side, SpanMultiTermQueryWrapper(headline:down??)], 0, true)");
    checkQuery(p, "slow-up??side down", "spanNear([headline:slow, SpanMultiTermQueryWrapper(headline:up??side), headline:down], 0, true)");
    checkQuery(p, "slow-up-??side down", "spanNear([headline:slow, headline:up, SpanMultiTermQueryWrapper(headline:??side), headline:down], 0, true)");

    checkQueryEqual(p, "up??side", fixRewrite(new WildcardQuery(new Term(field, "up??side"))), ignoreWC);

    // Based on real queries:
    checkQuery(p, "8-k*", "spanNear([headline:8, SpanMultiTermQueryWrapper(headline:k*)], 0, true)");
    checkQuery(p, "re-domesticat*", "spanNear([headline:re, SpanMultiTermQueryWrapper(headline:domesticat*)], 0, true)");
    checkQuery(p, "re-domicile*", "spanNear([headline:re, SpanMultiTermQueryWrapper(headline:domicile*)], 0, true)");
    checkQuery(p, "ipc-fipe*", "spanNear([headline:ipc, SpanMultiTermQueryWrapper(headline:fipe*)], 0, true)");
    checkQuery(p, "leo-mesdag b.v.*", "spanNear([headline:leo, headline:mesdag, headline:b.v], 0, true)");
    checkQuery(p, "spin-off*", "spanNear([headline:spin, SpanMultiTermQueryWrapper(headline:off*)], 0, true)");
    checkQuery(p, "skb-bank*", "spanNear([headline:skb, SpanMultiTermQueryWrapper(headline:bank*)], 0, true)");
    checkQuery(p, "gonzalez-paramo*", "spanNear([headline:gonzalez, SpanMultiTermQueryWrapper(headline:paramo*)], 0, true)");
    checkQuery(p, "jenaro cardona-fox*", "spanNear([headline:jenaro, headline:cardona, SpanMultiTermQueryWrapper(headline:fox*)], 0, true)");
    checkQuery(p, "t-note*", "spanNear([headline:t, SpanMultiTermQueryWrapper(headline:note*)], 0, true)");
    checkQuery(p, "non-bank*", "spanNear([headline:non, SpanMultiTermQueryWrapper(headline:bank*)], 0, true)");
    checkQuery(p, "conversion to open-end*", "spanNear([headline:conversion, headline:to, headline:open, SpanMultiTermQueryWrapper(headline:end*)], 0, true)");
    checkQuery(p, "estate uk-3*", "spanNear([headline:estate, headline:uk, SpanMultiTermQueryWrapper(headline:3*)], 0, true)");
    checkQuery(p, "sc-to-* sec filing", "spanNear([spanNear([headline:sc, headline:to], 0, true), spanNear([headline:sec, headline:filing], 0, true)], 1, true)");
    checkQuery(p, "ВСМПО-АВИСМА*", "spanNear([headline:всмпо, SpanMultiTermQueryWrapper(headline:ависма*)], 0, true)");
    checkQuery(p, "jean-franc* dubos", "spanNear([headline:jean, SpanMultiTermQueryWrapper(headline:franc*), headline:dubos], 0, true)");
    checkQuery(p, "vietnam-singapore industrial* park*", "spanNear([headline:vietnam, headline:singapore, SpanMultiTermQueryWrapper(headline:industrial*), SpanMultiTermQueryWrapper(headline:park*)], 0, true)");
    checkQuery(p, "prorated* or pro-rated", "spanNear([SpanMultiTermQueryWrapper(headline:prorated*), headline:or, headline:pro, headline:rated], 0, true)");

    q = new SpanTermQuery(new Term(field, "opiniao"));
    checkQueryEqual(p, "opinião", q, ignoreWC);
    q = fixRewrite(new PrefixQuery(new Term(field, "publico")));
    checkQueryEqual(p, "público*", q, ignoreWC);
    checkQuery(p, "público* opinião", "spanNear([SpanMultiTermQueryWrapper(headline:publico*), headline:opiniao], 0, true)");

    // Made up:
    checkQuery(p, "*or-me", "spanNear([SpanMultiTermQueryWrapper(headline:*or), headline:me], 0, true)");
    checkQuery(p, "or*-me", "spanNear([SpanMultiTermQueryWrapper(headline:or*), headline:me], 0, true)");

    // Based on real queries:
    checkQuery(p, "sc-to* sec filing", "spanNear([headline:sc, SpanMultiTermQueryWrapper(headline:to*), headline:sec, headline:filing], 0, true)");
    checkQuery(p, "throw-in*", "spanNear([headline:throw, SpanMultiTermQueryWrapper(headline:in*)], 0, true)");
  }

  public void testWildcardNearQueryParserStemming() throws Exception {
    String field = "body_en";
    boolean ignoreWC = false;
    WildcardNearQueryParser p = new WildcardNearQueryParser(field, new TestEnglishStemAnalyzer());

    checkQueryEqual(p, "*", new MatchAllDocsQuery(), ignoreWC);
    checkQueryEqual(p, "?", fixRewrite(new WildcardQuery(new Term(field, "?"))), ignoreWC);
    
    checkQuery(p, "cats", "body_en:cat");
    checkQuery(p, "cats*", "SpanMultiTermQueryWrapper(body_en:cats*)");
    checkQuery(p, "*cats", "SpanMultiTermQueryWrapper(body_en:*cats)");
    
    checkQuery(p, "cats-eye", "spanNear([body_en:cat, body_en:ey], 0, true)");
    checkQuery(p, "cats*eye", "SpanMultiTermQueryWrapper(body_en:cats*eye)");
  }
    
  public void testWildcardNearQueryParserKeyword() throws Exception {
    String field = "agent_name";
    boolean ignoreWC = false;
    WildcardNearQueryParser p = new WildcardNearQueryParser(field, new KeywordAnalyzer());

    checkQueryEqual(p, "*", new MatchAllDocsQuery(), ignoreWC);
    checkQueryEqual(p, "?", fixRewrite(new WildcardQuery(new Term(field, "?"))), ignoreWC);

    // Made up:
    checkQueryEqual(p, "London-City-Airport", new SpanTermQuery(new Term(field, "London-City-Airport")), ignoreWC);
    checkQueryEqual(p, "London City Airport", new SpanTermQuery(new Term(field, "London City Airport")), ignoreWC);
    checkQueryEqual(p, "London-Cit*-Airport", fixRewrite(new WildcardQuery(new Term(field, "London-Cit*-Airport"))), ignoreWC);
    checkQueryEqual(p, "London Cit* Airport", fixRewrite(new WildcardQuery(new Term(field, "London Cit* Airport"))), ignoreWC);

    checkQueryEqual(p, "Lon*", fixRewrite(new PrefixQuery(new Term(field, "Lon"))), ignoreWC);

    checkQueryEqual(p, "opinião", new SpanTermQuery(new Term(field, "opinião")), ignoreWC);
    checkQueryEqual(p, "público*", fixRewrite(new PrefixQuery(new Term(field, "público"))), ignoreWC);
    
    checkQueryEqual(p, "ngtr*", fixRewrite(new PrefixQuery(new Term(field, "ngtr"))), ignoreWC);
    checkQueryEqual(p, "bbotf*", fixRewrite(new PrefixQuery(new Term(field, "bbotf"))), ignoreWC);
}

  public void testWildcardNearQueryParserIgnoreWildcard() throws Exception {
    String field = "string";
    boolean ignoreWC = true;
    WildcardNearQueryParser p = new WildcardNearQueryParser(field, new KeywordAnalyzer());

    checkQueryEqual(p, "*", new SpanTermQuery(new Term(field, "*")), ignoreWC);
    checkQueryEqual(p, "?", new SpanTermQuery(new Term(field, "?")), ignoreWC);

    // Made up:
    checkQueryEqual(p, "London-City-Airport", new SpanTermQuery(new Term(field, "London-City-Airport")), ignoreWC);
    checkQueryEqual(p, "London City Airport", new SpanTermQuery(new Term(field, "London City Airport")), ignoreWC);
    checkQueryEqual(p, "London-Cit*-Airport", new SpanTermQuery(new Term(field, "London-Cit*-Airport")), ignoreWC);
    checkQueryEqual(p, "London Cit* Airport", new SpanTermQuery(new Term(field, "London Cit* Airport")), ignoreWC);

    checkQueryEqual(p, "Lon*", new SpanTermQuery(new Term(field, "Lon*")), ignoreWC);

    checkQueryEqual(p, "opinião", new SpanTermQuery(new Term(field, "opinião")), ignoreWC);
    checkQueryEqual(p, "público*", new SpanTermQuery(new Term(field, "público*")), ignoreWC);

    checkQueryEqual(p, "ngtr*", new SpanTermQuery(new Term(field, "ngtr*")), ignoreWC);
    checkQueryEqual(p, "bbotf*", new SpanTermQuery(new Term(field, "bbotf*")), ignoreWC);
  }

  public void testWildcardNearQueryParserJapaneseDiscardPunct() throws Exception {
    String field = "headline_ja";
    boolean ignoreWC = false;
    WildcardNearQueryParser p = new WildcardNearQueryParser(field, new TestJapaneseAnalyzerDiscardPunct());

    SpanNearQuery.Builder sqb = new SpanNearQuery.Builder(field, true /* ordered */);
    sqb.setSlop(0);
    sqb.addClause(new SpanTermQuery(new Term(field, "london")));
    sqb.addClause(fixRewrite(new PrefixQuery(new Term(field, "cit"))));
    sqb.addClause(new SpanTermQuery(new Term(field, "airport")));
    Query q2 = sqb.build();
    checkQueryEqual(p, "London Cit* Airport", q2, ignoreWC);
    checkQueryEqual(p, "London  Cit*  Airport  ", q2, ignoreWC);
    
    checkQueryEqual(p, "*", new MatchAllDocsQuery(), ignoreWC);
    checkQueryEqual(p, "?", fixRewrite(new WildcardQuery(new Term(field, "?"))), ignoreWC);

    // Made up:
    sqb = new SpanNearQuery.Builder(field, true /* ordered */);
    sqb.setSlop(0);
    sqb.addClause(new SpanTermQuery(new Term(field, "london")));
    sqb.addClause(new SpanTermQuery(new Term(field, "city")));
    sqb.addClause(new SpanTermQuery(new Term(field, "airport")));
    Query q = sqb.build();
    checkQueryEqual(p, "London-City-Airport", q, ignoreWC);
    checkQueryEqual(p, "London City Airport", q, ignoreWC);
    
    sqb = new SpanNearQuery.Builder(field, true /* ordered */);
    sqb.setSlop(0);
    sqb.addClause(new SpanTermQuery(new Term(field, "london")));
    sqb.addClause(fixRewrite(new PrefixQuery(new Term(field, "cit"))));
    sqb.addClause(new SpanTermQuery(new Term(field, "airport")));
    q = sqb.build();
    checkQueryEqual(p, "London-Cit*-Airport", q, ignoreWC);
    checkQueryEqual(p, "London Cit* Airport", q, ignoreWC);
    
    checkQueryEqual(p, "Lon*", fixRewrite(new PrefixQuery(new Term(field, "lon"))), ignoreWC);

    checkQuery(p,"1995 equity* 2003 equity* 2290 equity* 2306 equity* 2341 equity* " +
        "2427 equity* 2454 equity* 2812 equity* 2831 equity* 2927 equity* " +
        "3362 equity* 3375 equity* 3708 equity* 3740 equity* 4678 equity* " + 
        "5271 equity* 5280 equity* 5949 equity* 6286 equity* 6365 equity* " +
        "6476 equity* 6874 equity* 7219 equity* 7223 equity* 7265 equity* " +
        "7291 equity* 7292 equity* 7428 equity* 7473 equity* 7541 equity* " +
        "7718 equity* 7874 equity* 7880 equity* 8134 equity* 8208 equity* " +
        "8355 equity* 8358 equity* 8364 equity* 8567 equity* 9543 equity* " +
        "9817 equity* 9890 equity* 9939 equity* 9964 equity*",
        "spanNear([" +
        "headline_ja:1995, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:2003, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:2290, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:2306, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:2341, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:2427, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:2454, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:2812, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:2831, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:2927, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:3362, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:3375, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:3708, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:3740, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:4678, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:5271, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:5280, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:5949, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:6286, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:6365, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:6476, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:6874, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:7219, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:7223, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:7265, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:7291, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:7292, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:7428, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:7473, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:7541, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:7718, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:7874, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:7880, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:8134, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:8208, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:8355, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:8358, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:8364, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:8567, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:9543, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:9817, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:9890, SpanMultiTermQueryWrapper(headline_ja:equity*), " +
        "headline_ja:9939, SpanMultiTermQueryWrapper(headline_ja:equity*), headline_ja:9964, SpanMultiTermQueryWrapper(headline_ja:equity*)], 0, true" +
        ")");

    checkQueryEqual(p, "ngtr*", fixRewrite(new PrefixQuery(new Term(field, "ngtr"))), ignoreWC);
    checkQueryEqual(p, "bbotf*", fixRewrite(new PrefixQuery(new Term(field, "bbotf"))), ignoreWC);
  }

public void testWildcardNearQueryParserJapaneseDontDiscardPunct() throws Exception {
  String field = "body_ja";
  boolean ignoreWC = false;
  WildcardNearQueryParser p = new WildcardNearQueryParser(field, new JapaneseAnalyzerDontDiscardPunct());

  SpanNearQuery.Builder sqb = new SpanNearQuery.Builder(field, true /* ordered */);
  sqb.setSlop(0);
  sqb.addClause(new SpanTermQuery(new Term(field, "london")));
  sqb.addClause(fixRewrite(new PrefixQuery(new Term(field, "cit"))));
  sqb.addClause(new SpanTermQuery(new Term(field, "airport")));
  Query q2 = sqb.build();
  checkQueryEqual(p, "London Cit* Airport", q2, ignoreWC);
  checkQueryEqual(p, "London  Cit*  Airport  ", q2, ignoreWC);
  
  checkQueryEqual(p, "*", new MatchAllDocsQuery(), ignoreWC);
  checkQueryEqual(p, "?", fixRewrite(new WildcardQuery(new Term(field, "?"))), ignoreWC);

  // Made up:
  sqb = new SpanNearQuery.Builder(field, true /* ordered */);
  sqb.setSlop(0);
  sqb.addClause(new SpanTermQuery(new Term(field, "london")));
  sqb.addClause(new SpanTermQuery(new Term(field, "-")));
  sqb.addClause(new SpanTermQuery(new Term(field, "city")));
  sqb.addClause(new SpanTermQuery(new Term(field, "-")));
  sqb.addClause(new SpanTermQuery(new Term(field, "airport")));
  Query q = sqb.build();
  checkQueryEqual(p, "London-City-Airport", q, ignoreWC);

  sqb = new SpanNearQuery.Builder(field, true /* ordered */);
  sqb.setSlop(0);
  sqb.addClause(new SpanTermQuery(new Term(field, "london")));
  sqb.addClause(new SpanTermQuery(new Term(field, "city")));
  sqb.addClause(new SpanTermQuery(new Term(field, "airport")));
  q = sqb.build();
  checkQueryEqual(p, "London City Airport", q, ignoreWC);
  
  sqb = new SpanNearQuery.Builder(field, true /* ordered */);
  sqb.setSlop(0);
  sqb.addClause(new SpanTermQuery(new Term(field, "london")));
  sqb.addClause(fixRewrite(new PrefixQuery(new Term(field, "cit"))));
  sqb.addClause(new SpanTermQuery(new Term(field, "airport")));
  q = sqb.build();
  checkQueryEqual(p, "London Cit* Airport", q, ignoreWC);

  sqb = new SpanNearQuery.Builder(field, true /* ordered */);
  sqb.setSlop(0);
  sqb.addClause(new SpanTermQuery(new Term(field, "london")));
  sqb.addClause(new SpanTermQuery(new Term(field, "-")));
  sqb.addClause(fixRewrite(new WildcardQuery(new Term(field, "cit*-airport"))));
  q = sqb.build();
  checkQueryEqual(p, "London-Cit*-Airport", q, ignoreWC);

  checkQueryEqual(p, "Lon*", fixRewrite(new PrefixQuery(new Term(field, "lon"))), ignoreWC);

  checkQuery(p,"1995 equity* 2003 equity* 2290 equity* 2306 equity* 2341 equity* " +
      "2427 equity* 2454 equity* 2812 equity* 2831 equity* 2927 equity* " +
      "3362 equity* 3375 equity* 3708 equity* 3740 equity* 4678 equity* " + 
      "5271 equity* 5280 equity* 5949 equity* 6286 equity* 6365 equity* " +
      "6476 equity* 6874 equity* 7219 equity* 7223 equity* 7265 equity* " +
      "7291 equity* 7292 equity* 7428 equity* 7473 equity* 7541 equity* " +
      "7718 equity* 7874 equity* 7880 equity* 8134 equity* 8208 equity* " +
      "8355 equity* 8358 equity* 8364 equity* 8567 equity* 9543 equity* " +
      "9817 equity* 9890 equity* 9939 equity* 9964 equity*",
      "spanNear([" +
      "body_ja:1995, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:2003, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:2290, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:2306, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:2341, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:2427, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:2454, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:2812, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:2831, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:2927, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:3362, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:3375, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:3708, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:3740, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:4678, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:5271, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:5280, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:5949, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:6286, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:6365, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:6476, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:6874, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:7219, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:7223, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:7265, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:7291, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:7292, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:7428, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:7473, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:7541, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:7718, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:7874, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:7880, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:8134, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:8208, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:8355, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:8358, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:8364, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:8567, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:9543, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:9817, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:9890, SpanMultiTermQueryWrapper(body_ja:equity*), " +
      "body_ja:9939, SpanMultiTermQueryWrapper(body_ja:equity*), body_ja:9964, SpanMultiTermQueryWrapper(body_ja:equity*)], 0, true" +
      ")");

  checkQueryEqual(p, "ngtr*", fixRewrite(new PrefixQuery(new Term(field, "ngtr"))), ignoreWC);
  checkQueryEqual(p, "bbotf*", fixRewrite(new PrefixQuery(new Term(field, "bbotf"))), ignoreWC);
}
}
