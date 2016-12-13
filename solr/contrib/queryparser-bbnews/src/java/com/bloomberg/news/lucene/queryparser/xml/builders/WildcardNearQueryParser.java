package com.bloomberg.news.lucene.queryparser.xml.builders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Locale;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;


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

/* Builder for keyword phrases specially for wildcarded multi term queries.
 * Text phrases can be thrown into this builder to get tokenized to form OrderedNearQuery of sub queries of individual tokens.
 * Currently this can result in WildcardQuery,PrefixQuery and TermQuery as its sub queries. */

public class WildcardNearQueryParser {

  protected Analyzer analyzer;
  protected String field;
  private static final char WILDCARD_STRING = '*';
  private static final char WILDCARD_CHAR = '?';
  private static final char WILDCARD_ESCAPE = '\\';

  private enum WildcardState {
    TRAILING_WILDCARD,
    NON_TRAILING_WILDCARD,
    NON_WILDCARD
  }

  private enum WildcardType {
    STRING_WILDCARD,
    CHAR_WILDCARD,
    NOT_WILDCARD
  }

  public WildcardNearQueryParser(String field, Analyzer analyzer) {
    this.field = field;
    this.analyzer = analyzer;
  }

  public Query parse(String text) throws ParserException {
    return parse(text,false);
  }

  private boolean fieldHasOffsetAtt(TokenStream source) {
    return source.hasAttribute(PositionIncrementAttribute.class);
  }

  public Query parse(String unanalyzedText, boolean ignoreWildcard) throws ParserException {
    // to hold the sub queries until we generate the NearQuery
    ArrayList<SpanQuery> queries = new ArrayList<>();
    ArrayList<Integer> positions = new ArrayList<>();

    TokenStream source = null;
    try {
      source = analyzer.tokenStream(field, unanalyzedText);
      if (ignoreWildcard) {
        analysePhrases(source, unanalyzedText, queries, positions);
      } else if (!fieldHasOffsetAtt(source)) {
        SpanQuery q = BuildWildcardQuery(unanalyzedText);
        if (q != null) {
          queries.add(q);
          positions.add(0);
        }
      } else {
        // The field has an offsetAtt
        analyseWildcardPhrases(source, unanalyzedText, queries, positions);
      }
    } finally {
      IOUtils.closeWhileHandlingException(source);
    }

    if(queries.isEmpty()) {
      return new MatchAllDocsQuery();
    } else {
      return makeSubQuery(queries, positions, 0, queries.size()-1);
    }
  }

  //
  private SpanQuery makeSubQuery(ArrayList<SpanQuery> queries, ArrayList<Integer> positions, int startIndex, int stopIndex){
    if(startIndex == stopIndex)
      return queries.get(startIndex);

    int index = startIndex;
    final int groupable_distance = 1; //if the tokens are far apart than this distance then it is a candidate for near query.
    while (index < stopIndex) {
      int distance = positions.get(index+1)- positions.get(index);
      if(distance == groupable_distance){
        index++;
        continue;
      }
      else {
        SpanQuery[] subQueries = new SpanQuery[2];
        subQueries[0] = makeSubQuery(queries, positions, startIndex, index);
        subQueries[1] = makeSubQuery(queries, positions, index+1, stopIndex);
        return new SpanNearQuery(subQueries, distance-1, true);
      }
    }

    final int numberOfElementstoCopy= stopIndex-startIndex+1;
    SpanQuery[] subQueries = new SpanQuery[numberOfElementstoCopy];
    for(int i=0; i < numberOfElementstoCopy; i++)
    {
      subQueries[i] = queries.get(startIndex + i);
    }
    return new SpanNearQuery(subQueries, groupable_distance-1/*slop*/, true);
  }

  //analyses the text and fills up the individual term queries and positions in the respective array lists
  //and returns the position increment after the last query token. This increment can be used to chain up the rest of the phrases to this query list.
  private int analysePhrases(TokenStream source, String unanalyzedText,
      ArrayList<SpanQuery> queries, ArrayList<Integer> positions) throws ParserException {
    int currentPosition = -1;
    try {
      source.reset();

      TermToBytesRefAttribute termAtt = null;
      BytesRef bytes = null;
      if (source.hasAttribute(TermToBytesRefAttribute.class)) {
        termAtt = source.getAttribute(TermToBytesRefAttribute.class);
      } else throw new ParserException(
          "Cannot build query token stream has no TermToBytesRefAttribute. field:"
              + field + ", text:" + unanalyzedText);

      PositionIncrementAttribute posIncrAtt = null;
      if (source.hasAttribute(PositionIncrementAttribute.class)) {
        posIncrAtt = source.getAttribute(PositionIncrementAttribute.class);
      }

      while (source.incrementToken()) {
        int positionIncrement = (posIncrAtt != null) ? posIncrAtt
            .getPositionIncrement() : 1;
        currentPosition += positionIncrement;
        bytes = termAtt.getBytesRef();
        SpanQuery q = new SpanTermQuery(new Term(field, BytesRef.deepCopyOf(bytes)));
        if (q != null) {
            queries.add(q);
        }
        positions.add(currentPosition);
      }
      source.end();
      if (posIncrAtt != null)
        currentPosition += posIncrAtt.getPositionIncrement();
    } catch (IOException ioe) {
      ParserException p = new ParserException("Cannot build query on field:"
          + field + ", text:" + unanalyzedText);
      p.initCause(ioe);
      throw p;
    } finally {
      IOUtils.closeWhileHandlingException(source);
    }
    return currentPosition;
  }

  private String sourceSubstring(String unanalyzedText, int begin, int end) {
    // Since this is original (pre-analyzer) unanalyzedText, it has not been lowercased.
    // Assume that we want it in lowercase. Future: pass information from the
    // analyzer indicating whether to lowercase original unanalyzedText or not.
      return unanalyzedText.substring(begin, end).toLowerCase(Locale.getDefault());
  }

  private boolean isAllWhitespace(BytesRef bytes) {
    String full = bytes.utf8ToString();
    return full.matches("^\\s*$");
  }

  //analyses the text and fills up the individual term queries and
  //positions in the respective array lists and returns the position
  //increment after the last query token. This increment can be used
  //to chain up the rest of the phrases to this query list.
  private int analyseWildcardPhrases(TokenStream source, String unanalyzedText,
     ArrayList<SpanQuery> queries, ArrayList<Integer> positions) throws ParserException {
    int currentPosition = -1;
    ArrayList<WildcardType> wildcards = findWildcards(unanalyzedText);
    try {
      source.reset();

      // termAtt and bytes will get updated as a side-effect of
      // calling source.incrementToken()
      TermToBytesRefAttribute termAtt = null;
      BytesRef bytes = null;
      if (source.hasAttribute(TermToBytesRefAttribute.class)) {
        termAtt = source.getAttribute(TermToBytesRefAttribute.class);
      } else throw new ParserException(
          "Cannot build query token stream has no TermToBytesRefAttribute. field:"
              + field + ", text:" + unanalyzedText);

      // posIncrAtt will get updated as a side-effect of calling
      // source.incrementToken()
      PositionIncrementAttribute posIncrAtt = null;
      if (source.hasAttribute(PositionIncrementAttribute.class)) {
        posIncrAtt = source.getAttribute(PositionIncrementAttribute.class);
      }

      // offsetAtt will get updated as a side-effect of calling
      // source.incrementToken()
      OffsetAttribute offsetAtt = null;
      if (source.hasAttribute(PositionIncrementAttribute.class)) {
        offsetAtt = source.getAttribute(OffsetAttribute.class);
      }

      // The analyzer will split our phrase into pieces and hide
      // wildcards.  We will be gluing some of these pieces back
      // together and need to do some bookkeeping.
      String reconstructed = "";
      int lastOffset = 0;

      // The splitting into pieces and re-glueing things together will
      // all require extra bookkeeping to get the proper
      // positionIncrements.
      int lastPositionIncrement = 1;

      while (source.incrementToken()) {
        int positionIncrement = (posIncrAtt != null) ? posIncrAtt
            .getPositionIncrement() : 1;
        lastPositionIncrement = positionIncrement;

        // Look at the characters after the end of the previous token
        // and before the beginning of this one.
        // If there are wildcards, we may need to
        // either: (a) increment currentPosition or (b) merge the
        // wildcards with the previous token or the current token or
        // both.
        for (int j = lastOffset; j < offsetAtt.startOffset(); j++) {
          if (wildcards.get(j) == WildcardType.NOT_WILDCARD) {
            if (!reconstructed.isEmpty()) {
              // Not a wildcard. In the process of merging, so merge ends now.
              lastOffset = j;
              currentPosition += lastPositionIncrement;
              lastPositionIncrement = 1;
              SpanQuery wq = BuildWildcardQuery(reconstructed);
              if (wq != null) {
                queries.add(wq);
                positions.add(currentPosition);
              }
              reconstructed = "";
            }
          } else {
            reconstructed += sourceSubstring(unanalyzedText, j, j+1);
            lastOffset = j + 1;
          }
        }

        bytes = termAtt.getBytesRef();
        lastOffset = offsetAtt.endOffset();
        if (isAllWhitespace(bytes)) {
          // On an all-whitespace token, this breaks concatentation and drops the token.
          // In the future, we will not have all-whitespace tokens reaching this code and
          // we could remove it.
          if (!reconstructed.isEmpty()) {
            // Not a wildcard. If in the process of merging, then merge ends now.
            lastOffset = offsetAtt.startOffset();
            currentPosition += lastPositionIncrement;
            lastPositionIncrement = 1;
            SpanQuery wq = BuildWildcardQuery(reconstructed);
            if (wq != null) {
              queries.add(wq);
              positions.add(currentPosition);
            }
            reconstructed = "";
          }
        } else if ((!reconstructed.isEmpty()) ||
                   (lastOffset < wildcards.size() &&
                    wildcards.get(lastOffset) != WildcardType.NOT_WILDCARD)) {
          // Current token will need to merge with at least one following
          // wildcard. Don't create the term yet.
          int unanalyzedLength = (offsetAtt.endOffset() - offsetAtt.startOffset());
          if (unanalyzedLength > bytes.length) {
              // reverse the effect of stemming
              reconstructed += sourceSubstring(unanalyzedText, offsetAtt.startOffset(), offsetAtt.endOffset());
          } else {
              reconstructed += bytes.utf8ToString();
          }
        } else {
          // Current token doesn't need to merge with anything before
          // or after it, so create it.
          currentPosition += lastPositionIncrement;
          lastPositionIncrement = 1;
          SpanQuery q = BuildWildcardQuery(bytes.utf8ToString());
          if (q != null) {
            queries.add(q);
            positions.add(currentPosition);
          }
        }
      }

      // We are done with the non-wildcard tokens. But, there may still be wildcards.
      for (int j = lastOffset; j < wildcards.size(); j++) {
        if (wildcards.get(j) == WildcardType.NOT_WILDCARD) {
          if (!reconstructed.isEmpty()) {
            // End of current merge
            currentPosition += lastPositionIncrement;
            lastPositionIncrement = 1;
            lastOffset = j;
            SpanQuery wq = BuildWildcardQuery(reconstructed);
            if (wq != null) {
              queries.add(wq);
              positions.add(currentPosition);
            }
            reconstructed = "";
          }
        } else {
          reconstructed += sourceSubstring(unanalyzedText, j, j+1);
          lastOffset = j + 1;
        }
      }

      // We are done with all the tokens. Create a final term if necessary.
      if (!reconstructed.isEmpty()) {
        currentPosition += lastPositionIncrement;
        lastPositionIncrement = 1;
        lastOffset = wildcards.size();
        SpanQuery wq = BuildWildcardQuery(reconstructed);
        if (wq != null) {
          queries.add(wq);
          positions.add(currentPosition);
        }
        reconstructed = "";
      }

      source.end();
      if (posIncrAtt != null)
        currentPosition += posIncrAtt.getPositionIncrement();
    } catch (IOException ioe) {
      ParserException p = new ParserException("Cannot build query on field:"
          + field + ", text:" + unanalyzedText);
      p.initCause(ioe);
      throw p;
    } finally {
      IOUtils.closeWhileHandlingException(source);
    }
    return currentPosition;
  }

  private ArrayList<WildcardType> findWildcards(String text) {
    ArrayList<WildcardType> wildcards = new ArrayList<>();
    for (int i = 0; i < text.length();) {
      final int c = text.codePointAt(i);
      int length = Character.charCount(c);
      switch (c) {
        case WILDCARD_CHAR:
          wildcards.add(WildcardType.CHAR_WILDCARD);
          break;
        case WILDCARD_STRING:
          wildcards.add(WildcardType.STRING_WILDCARD);
          break;
        case WILDCARD_ESCAPE:
          wildcards.add(WildcardType.NOT_WILDCARD);
          // skip over the escaped code point if it exists
          if (i + length < text.length()) {
            final int nextChar = text.codePointAt(i + length);
            length += Character.charCount(nextChar);
          }
          break;
        default:
          wildcards.add(WildcardType.NOT_WILDCARD);
      }
      for (int j = 1; j < length; j++) {
        wildcards.add(WildcardType.NOT_WILDCARD);
      }
      i += length;
    }
    return wildcards;
  }

  private static WildcardState checkWildcard(String text) {
    for (int i = 0; i < text.length();) {
      final int c = text.codePointAt(i);
      int length = Character.charCount(c);
      switch (c) {
        case WILDCARD_STRING:
          if (i + length == text.length())
            return WildcardState.TRAILING_WILDCARD;
          else
            return WildcardState.NON_TRAILING_WILDCARD;
        case WILDCARD_CHAR:
            return WildcardState.NON_TRAILING_WILDCARD;
        case WILDCARD_ESCAPE:
          // skip over the escaped code point if it exists
          if (i + length < text.length()) {
            final int nextChar = text.codePointAt(i + length);
            length += Character.charCount(nextChar);
          }
          break;
      }
      i += length;
    }
    return WildcardState.NON_WILDCARD;
  }

  private static boolean equalsWildcardString(String text) {
    // If text has only one WILDCARD_STRINGs,
    // then it is equivalent to a simple WILDCARD_STRING.
    boolean aWildcardString = false;
    for (int i = 0; i < text.length(); i++) {
      final int c = text.codePointAt(i);
      switch (c) {
        case WILDCARD_STRING:
          aWildcardString = true;
          break;
        default:
          return false;
      }
    }
    return aWildcardString;
  }

  private SpanQuery BuildWildcardQuery(String token) {
    if (equalsWildcardString(token)) {
      // We don't need to build an actual Query for these
      return null;
    }
    WildcardState ws = checkWildcard(token);
    if (ws == WildcardState.NON_WILDCARD) {
      return new SpanTermQuery(new Term(field, token));
    }
    if (token.length() > 1 && ws == WildcardState.TRAILING_WILDCARD) {
      PrefixQuery pq = new PrefixQuery(new Term(field, token.substring(
          0, token.length() - 1)));
      pq.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE);
      return new SpanMultiTermQueryWrapper<PrefixQuery>(pq);
    }
    WildcardQuery wq = new WildcardQuery(new Term(field, token));
    wq.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE);
    return new SpanMultiTermQueryWrapper<WildcardQuery>(wq);
  }
}
