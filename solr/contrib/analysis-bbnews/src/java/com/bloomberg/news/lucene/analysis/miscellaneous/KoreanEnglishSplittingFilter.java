package com.bloomberg.news.lucene.analysis.miscellaneous;

import java.io.IOException;
import java.util.LinkedList;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Splits along Hangul/ASCII alphanumeric boundaries
 */

public final class KoreanEnglishSplittingFilter extends TokenFilter {

    private static final Pattern NUMERIC_PATTERN = Pattern.compile("[0-9]*\\.?[0-9]+");

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    private final boolean incrementPositionForNumbers;
    private final boolean preserveOriginal;

    private LinkedList<String> splitText = new LinkedList<String>();
    private boolean containsNumber = false;
    private int startOffset = 0;
    private int endOffset = 0;
    private boolean previousWasOriginal = false;

    public KoreanEnglishSplittingFilter(TokenStream input) {
        this(input, true, false);
    }

    public KoreanEnglishSplittingFilter(TokenStream input, boolean incrementPositionForNumbers) {
        this(input, incrementPositionForNumbers, false);
    }

    /**
     * Create a new {@link KoreanEnglishSplittingFilter}.
     *
     * @param input
     *          TokenStream to filter
     * @param incrementPositionForNumbers
     *          fudge factor, when set to false puts split tokens at the same position if input contains numbers
     * @param preserveOriginal
     *          should the original tokens be kept on the input stream with a 0 position increment
     *          from the split tokens
     **/
    public KoreanEnglishSplittingFilter(TokenStream input, boolean incrementPositionForNumbers, boolean preserveOriginal) {
        super(input);
        this.incrementPositionForNumbers = incrementPositionForNumbers;
        this.preserveOriginal = preserveOriginal;
    }

    private enum CharType {
        HANGUL, ALNUM, OTHER
    }

    private boolean isHangul(int c) {
        if ( (c >= '\u1100' && c <= '\u11ff') ||
             (c >= '\u302e' && c <= '\u302f') ||
             (c >= '\u3131' && c <= '\u318e') ||
             (c >= '\u3200' && c <= '\u321e') ||
             (c >= '\u3260' && c <= '\u327e') ||
             (c >= '\ua960' && c <= '\ua97c') ||
             (c >= '\uac00' && c <= '\ud7a3') ||
             (c >= '\ud7b0' && c <= '\ud7c6') ||
             (c >= '\ud7cb' && c <= '\ud7fb') ||
             (c >= '\uffa0' && c <= '\uffbe') ||
             (c >= '\uffc2' && c <= '\uffc7') ||
             (c >= '\uffca' && c <= '\uffcf') ||
             (c >= '\uffd2' && c <= '\uffd7') ||
             (c >= '\uffda' && c <= '\uffdc') )
            return true;

        return false;
    }

    private boolean isAlnum(int c) {
        if ( (c >= 65 && c <= 90) || (c >= 97 && c <= 122) || (c >= 48 && c <= 57) )
            return true;
        return false;
    }
    private CharType getCharType(int c) {
        if (isHangul(c))
            return CharType.HANGUL;
        if (isAlnum(c))
            return CharType.ALNUM;
        return CharType.OTHER;
    }


    @Override
    public boolean incrementToken() throws IOException {

        if (!splitText.isEmpty()) {
            clearAttributes();

            String term = splitText.remove(0);
            termAtt.setEmpty().append(term);
            offsetAtt.setOffset(startOffset, endOffset);

            if (preserveOriginal && previousWasOriginal) {
              previousWasOriginal = false;
              posIncAtt.setPositionIncrement(0); // place 1st split at same pos as original
            }

            if (containsNumber &&  incrementPositionForNumbers == false) {
              posIncAtt.setPositionIncrement(0);
            }

            return true;
        }

        containsNumber = false;

        if (input.incrementToken()) {
            String text = termAtt.toString();
            if (text.isEmpty()) {
                return true;
            }

            int prevOffset = 0, curOffset = 0;
            CharType prevType = CharType.OTHER, curType = CharType.OTHER;

            int codepoint = text.codePointAt(curOffset);
            curOffset += Character.charCount(codepoint);
            curType = getCharType(codepoint);
            prevType = curType;

            int startTerm = 0;
            for (; curOffset < text.length();) {
                codepoint = text.codePointAt(curOffset);
                curType = getCharType(codepoint);

                if (curType != prevType && curType != CharType.OTHER && prevType != CharType.OTHER) {
                    splitText.add(text.substring(startTerm, curOffset));
                    startTerm = curOffset;
                }

                prevOffset = curOffset;
                prevType = curType;
                curOffset += Character.charCount(codepoint);
            }

            if (splitText.size() == 0) {
                return true; // no splitting was needed so there's nothing more for this filter to do
            }

            splitText.add(text.substring(startTerm));   // add the last remaining piece to the list

            startOffset = offsetAtt.startOffset();
            endOffset = offsetAtt.endOffset();

            if (preserveOriginal) {
                // return original token on this pass and set a flag for the next pass
                previousWasOriginal = true;
            }
            else {
                String term = splitText.remove(0);
                termAtt.setEmpty().append(term);
                if (!incrementPositionForNumbers && NUMERIC_PATTERN.matcher(text).find()) {
                  containsNumber = true;
                }
            }

            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        splitText.clear();
        containsNumber = false;
        startOffset = 0;
        endOffset = 0;
        previousWasOriginal = false;
    }
}
