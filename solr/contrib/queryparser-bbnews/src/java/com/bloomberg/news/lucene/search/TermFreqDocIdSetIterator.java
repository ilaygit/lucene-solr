package com.bloomberg.news.lucene.search;

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

import java.io.IOException;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.Scorer;

/**
 * This abstract class wraps a {@link PostingsEnum} object and defines methods to
 * iterate over its set of doc ids, matching based on documents' term frequency.
 *
 * <b>NOTE:</b>
 * <code>match()</code>-ing is implemented via calls to <code>PostingsEnum.freq()</code>.
 * If the {@link PostingsEnum} was obtained without <code>PostingsEnum.FLAG_FREQS</code>
 * the result of the matching are undefined.
 */
public class TermFreqDocIdSetIterator extends CustomDocIdSetIterator {

  private final IntegerRange termFreqRange;

  /**
   * Returns whether or not the current document's term frequency falls within
   * the <code>termFreqRange</code> range.
   */
  public boolean match() throws IOException {
    return
        termFreqRange == null ||
        termFreqRange.includes(scorer().freq());
  }

  public TermFreqDocIdSetIterator(Scorer scorer, IntegerRange termFreqRange) {
    super(scorer);
    this.termFreqRange = termFreqRange;
  }

}
