package com.bloomberg.news.lucene.analysis;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

//analyser filter for considering the last payload value as the term frequency
public class RepeatingTokenFilter extends TokenFilter {

  private final char              delimiter;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  private int repeatCount = 0;

  public RepeatingTokenFilter(TokenStream input, char delimiter) {
    super(input);
    this.delimiter = delimiter;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (repeatCount <= 0) {

      if (!input.incrementToken()) {
        return false;
      }

      final char[] buffer = termAtt.buffer();
      final int length = termAtt.length();

      // at least one char required before and after delimiter
      for (int ll = 1; ll <= length-2; ll++) {
        if (buffer[ll] != delimiter) continue;
        for (int rr = length-2; rr >= ll; rr--) {
          if (buffer[rr] != delimiter) continue;

          final int offset = rr + 1;
          final String sToken = new String(buffer, offset, length-offset);

          repeatCount = Integer.parseInt(sToken);
          if (repeatCount > 0) {
            repeatCount--;
            termAtt.setLength(ll); // truncate "term" to remove all the relevance values
            return true;
          } else {
            throw new IOException("repeatCount="+repeatCount+" found in term value: " + termAtt.toString());
          }
        }
      }

      throw new IOException("failed to get payload value from term [" + termAtt.toString() + "]");

    } else {

      --repeatCount;
      return true;

    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    repeatCount = 0;
  }
}
