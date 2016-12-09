package com.bloomberg.news.lucene.analysis;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.BeforeClass;

public class TestRepeatingTokenFilter extends LuceneTestCase {

  private static RepeatingTokenFilterFactory factory;

  @BeforeClass
  public static void beforeClass() throws Exception {
    factory = new RepeatingTokenFilterFactory(new HashMap<String,String>());
  }

  public final void testDefaultDelimiter() throws IOException
  {
    assertEquals(':', RepeatingTokenFilterFactory.DELIMITER_DEFAULT);
    final RepeatingTokenFilterFactory rtff = new RepeatingTokenFilterFactory(new HashMap<String,String>());
    assertEquals(RepeatingTokenFilterFactory.DELIMITER_DEFAULT, rtff.getDelimiter());
  }

  public final void testDelimiter() throws IOException
  {
    final char delimiterChar = (random().nextBoolean() ? RepeatingTokenFilterFactory.DELIMITER_DEFAULT : '@');
    final String delimiterString = new String(new char[] {delimiterChar});

    final Map<String,String> initParams = new HashMap<>();
    initParams.put(RepeatingTokenFilterFactory.DELIMITER_ATTR, delimiterString);

    final RepeatingTokenFilterFactory rtff = new RepeatingTokenFilterFactory(initParams);
    assertEquals(delimiterChar, rtff.getDelimiter());
  }

  public final void testRepeatingToken() throws IOException
  {
    final String termValue = "oil";
    final int frequency = 1+random().nextInt(10);
    final String input = termValue + factory.getDelimiter() + frequency;
    implTestRepeatingToken(input, termValue, frequency, null);
  }

  public final void testRepeatingTokenWithMultipleDelimiters() throws IOException
  {
    final String termValue = "oil";
    int frequency = 1+random().nextInt(10);
    String input = termValue + factory.getDelimiter() + frequency;
    do {
      frequency = 1+random().nextInt(10);
      input = input + factory.getDelimiter() + frequency;
    } while (random().nextBoolean());
    implTestRepeatingToken(input, termValue, frequency, null);
  }

  public final void testRepeatingTokenWithWrongDelimiter() throws IOException
  {
    final char wrongDelimiter = '?';
    assertNotSame(factory.getDelimiter(), wrongDelimiter);
    final String termValue = "oil";
    final int frequency = 1+random().nextInt(10);
    final String input = termValue + wrongDelimiter + frequency;
    final IOException expectedIOException = new IOException("failed to get payload value from term [" + input + "]");
    implTestRepeatingToken(input, termValue, 0, expectedIOException);
  }

  public final void testRepeatingTokenWithEmptyTerm() throws IOException
  {
    final String termValue = "";
    final int frequency = 1+random().nextInt(10);
    final String input = termValue + factory.getDelimiter() + frequency;
    final IOException expectedIOException = new IOException("failed to get payload value from term [" + input + "]");
    implTestRepeatingToken(input, termValue, 0, expectedIOException);
  }

  public final void testRepeatingTokenWithZeroFrequency() throws IOException
  {
    final String termValue = "zero";
    final int frequency = 0;
    final String input = termValue + factory.getDelimiter() + frequency;
    final IOException expectedIOException = new IOException("repeatCount="+frequency+" found in term value: "+input);
    implTestRepeatingToken(input, termValue, frequency, expectedIOException);
  }

  public final void testRepeatingTokenWithMissingFrequency() throws IOException
  {
    final String termValue = "missing";
    final String input = termValue + factory.getDelimiter();
    final IOException expectedIOException = new IOException("failed to get payload value from term [" + input + "]");
    implTestRepeatingToken(input, termValue, 0, expectedIOException);
  }

  private final void implTestRepeatingToken(String input, String expectedTokenValue, int expectedTokenCount, IOException expectedIOException) throws IOException
  {
    final StringReader reader = new StringReader(input);
    try (Tokenizer mToken = new MockTokenizer();
        TokenStream stream = factory.create(mToken)) {

      mToken.setReader(reader);
      CharTermAttribute termAtt = null;
      assertTrue("has no CharTermAttribute", stream.hasAttribute(CharTermAttribute.class));
      termAtt = stream.getAttribute(CharTermAttribute.class);

      stream.reset();
      int tokenCount = 0;
      boolean reachedEnd = false;
      while (!reachedEnd) {
        try {
          if (stream.incrementToken()) {
            assertEquals("term "+tokenCount, expectedTokenValue, termAtt.toString());
            ++tokenCount;
          } else {
            reachedEnd = true;
          }
        } catch (IOException actualIOException) {
          if (expectedIOException == null) {
            fail("unexpected IOException "+actualIOException);
          } else {
            assertEquals(expectedIOException.toString(), actualIOException.toString());
          }
        }
      }
      assertEquals("tokenCount", expectedTokenCount, tokenCount);
      stream.end();
    }
  }

}
