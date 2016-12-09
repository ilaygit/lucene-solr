package com.bloomberg.news.lucene.analysis.miscellaneous;

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

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for {@link KoreanEnglishSplittingFilter}.
 *
 * @see KoreanEnglishSplittingFilter
 */
public class KoreanEnglishSplittingFilterFactory extends TokenFilterFactory {

  protected final boolean incrementPositionForNumbers;
  protected final boolean preserveOriginal;

  /** Creates a new TrimFilterFactory */
  public KoreanEnglishSplittingFilterFactory(Map<String,String> args) {
    super(args);
    incrementPositionForNumbers = getBoolean(args, "incrementPositionForNumbers", true);
    preserveOriginal = getBoolean(args, "preserveOriginal", false);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public KoreanEnglishSplittingFilter create(TokenStream input) {
    final KoreanEnglishSplittingFilter filter =
          new KoreanEnglishSplittingFilter(input, incrementPositionForNumbers, preserveOriginal);
    return filter;
  }
}
