package org.dcache.util;

import com.google.common.base.Joiner;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import static org.dcache.util.ChecksumType.*;
import org.hamcrest.Description;
import static org.hamcrest.Matchers.equalTo;
import org.hamcrest.TypeSafeMatcher;
import static org.junit.Assert.assertThat;
import org.junit.Test;

public class ChecksumsTests
{
    private Checksum _checksum;
    private String _rfc3230;
    private Collection<Checksum> _checksums;

    @Test
    public void shouldGiveEmptyStringForUnsupportedChecksumType()
    {
        givenSingle(checksum().ofType(MD4_TYPE).
                withValue("6df23dc03f9b54cc38a0fc1483df6e21"));

        whenGeneratingRfc3230ForSingleChecksum();

        assertThat(_rfc3230, equalTo(""));
    }

    @Test
    public void shouldGiveCorrectStringForAdler32()
    {
        givenSingle(checksum().ofType(ADLER32).withValue("03da0195"));

        whenGeneratingRfc3230ForSingleChecksum();

        assertThat(_rfc3230, equalTo("adler32:03da0195"));
    }

    @Test
    public void shouldGiveCorrectStringForShortAdler32()
    {
        givenSingle(checksum().ofType(ADLER32).withValue("3da0195"));

        whenGeneratingRfc3230ForSingleChecksum();

        assertThat(_rfc3230, equalTo("adler32:03da0195"));
    }


    @Test
    public void shouldGiveCorrectStringForMD5()
    {
        givenSingle(checksum().ofType(MD5_TYPE).
                withValue("1d45d92d02ccb88fca6792837093dc38"));

        whenGeneratingRfc3230ForSingleChecksum();

        assertThat(_rfc3230, equalTo("md5:HUXZLQLMuI/KZ5KDcJPcOA=="));
    }

    @Test
    public void shouldGiveEmptyStringForSetOfUnsupportedType()
    {
        givenSet(checksum().ofType(MD4_TYPE).
                withValue("6df23dc03f9b54cc38a0fc1483df6e21"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, equalTo(""));
    }

    @Test
    public void shouldGiveEmptyStringForEmptySetOfChecksums()
    {
        givenSet();

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, equalTo(""));
    }

    @Test
    public void shouldGiveCorrectStringForSetOfAdler32()
    {
        givenSet(checksum().ofType(ADLER32).withValue("3da0195"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, equalTo("adler32:03da0195"));
    }

    @Test
    public void shouldGiveCorrectStringForSetOfMd5()
    {
        givenSet(checksum().ofType(MD5_TYPE).
                withValue("1d45d92d02ccb88fca6792837093dc38"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, equalTo("md5:HUXZLQLMuI/KZ5KDcJPcOA=="));
    }

    @Test
    public void shouldGiveCorrectStringForAdler32AndMD5()
    {
        givenSet(checksum().ofType(ADLER32).withValue("3da0195"),
                checksum().ofType(MD5_TYPE).
                        withValue("1d45d92d02ccb88fca6792837093dc38"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, hasOnlyParts("adler32:03da0195",
                "md5:HUXZLQLMuI/KZ5KDcJPcOA=="));
    }

    @Test
    public void shouldGiveCorrectStringForUnsupportedAndAdler32AndMD5()
    {
        givenSet(checksum().ofType(MD4_TYPE).
                        withValue("6df23dc03f9b54cc38a0fc1483df6e21"),
                checksum().ofType(ADLER32).withValue("3da0195"),
                checksum().ofType(MD5_TYPE).
                        withValue("1d45d92d02ccb88fca6792837093dc38"));

        whenGeneratingRfc3230ForSetOfChecksums();

        assertThat(_rfc3230, hasOnlyParts("adler32:03da0195",
                "md5:HUXZLQLMuI/KZ5KDcJPcOA=="));
    }

    private void givenSingle(ChecksumBuilder builder)
    {
        _checksum = builder.build();
    }

    private void givenSet(ChecksumBuilder... builders)
    {
        _checksums = new HashSet<>();

        for(ChecksumBuilder builder : builders) {
            _checksums.add(builder.build());
        }
    }

    private void whenGeneratingRfc3230ForSingleChecksum()
    {
        _rfc3230 = Checksums.rfc3230Encoded(_checksum);
    }

    private void whenGeneratingRfc3230ForSetOfChecksums()
    {
        _rfc3230 = Checksums.rfc3230Encoded(_checksums);
    }

    private ChecksumBuilder checksum()
    {
        return new ChecksumBuilder();
    }

    private class ChecksumBuilder
    {
        private ChecksumType _type;
        private String _value;

        public ChecksumBuilder ofType(ChecksumType type)
        {
            checkNotNull(type);
            _type = type;
            return this;
        }

        public ChecksumBuilder withValue(String value)
        {
            checkNotNull(value);
            _value = value;
            return this;
        }

        public Checksum build()
        {
            checkNotNull(_value);
            checkNotNull(_type);
            return new Checksum(_type, _value);
        }
    }

    private static HasOnlyParts hasOnlyParts(String... parts)
    {
        return new HasOnlyParts(parts);
    }

    /**
     * Matcher that passes if the supplied comma-separated list of parts
     * contains all of the matching parts and nothing else.  The order of
     * the parts does not matter.
     */
    private static class HasOnlyParts extends TypeSafeMatcher<String>
    {
        private Set<String> _needles = new HashSet<>();
        private String _missing;
        private String _extra;

        public HasOnlyParts(String... parts)
        {
            _needles.addAll(Arrays.asList(parts));
        }

        @Override
        protected boolean matchesSafely(String t)
        {
            Set<String> haystack = Sets.newHashSet(Splitter.on(',').split(t));

            if(!haystack.containsAll(_needles)) {
                _needles.removeAll(haystack);
                _missing = Joiner.on(", ").join(_needles);
                return false;
            }

            if(!_needles.containsAll(haystack)) {
                haystack.removeAll(_needles);
                _extra = Joiner.on(", ").join(haystack);
                return false;
            }

            return true;
        }

        @Override
        public void describeTo(Description d)
        {
            if(_missing != null) {
                d.appendText("missing: ").appendValue(_missing);
            } else {
                d.appendText("unexpected: ").appendValue(_extra);
            }
        }

    }
}