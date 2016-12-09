package com.bloomberg.news.lucene.analysis.standard;

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

import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.junit.BeforeClass;

public class TestFinancialStandardTokenizer extends BaseTokenStreamFactoryTestCase {

  private static Map<String,String> tokenizerFactoryArgs;
  private static TokenizerFactory tokenizerFactory;

  @BeforeClass
  public static void beforeClass() throws Exception {
    tokenizerFactoryArgs = new HashMap<String,String>();
    tokenizerFactory = new FinancialStandardTokenizerFactory(tokenizerFactoryArgs);
  }

  public void testFinancialStandardTokenizer() throws Exception {
    final Reader reader = new StringReader("What's this thing do?");
    final Tokenizer tokenizer = tokenizerFactory.create();
    tokenizer.setReader(reader);
    assertTokenStreamContents(tokenizer,
        new String[] { "What's", "this", "thing", "do" });
  }

  public void testFinancialStandardTokenizerCurrencySymbols() throws Exception {
    final int amount = random().nextInt(100);
    final String pound = new String("\u00A3");
    final String dollar = new String("$");
    final String euro = new String("\u20AC");
    final String yen = new String("\u00A5");
    final Reader reader = new StringReader(pound+amount+" "+dollar+amount+" "+euro+amount+" "+yen+amount);
    final Tokenizer tokenizer = tokenizerFactory.create();
    tokenizer.setReader(reader);
    assertTokenStreamContents(tokenizer,
        new String[] { pound+amount, dollar+amount, euro+amount, yen+amount });
  }

}
