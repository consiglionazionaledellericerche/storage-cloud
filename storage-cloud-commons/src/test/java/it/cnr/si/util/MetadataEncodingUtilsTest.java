/*
 * Copyright (C) 2019  Consiglio Nazionale delle Ricerche
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as
 *     published by the Free Software Foundation, either version 3 of the
 *     License, or (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package it.cnr.si.util;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by marco.spasiano on 19/07/17.
 */

public class MetadataEncodingUtilsTest {

    public static final String KEY = "foo_pippo:bar-baz";
    public static final String ENCODED_KEY = "CNR_Zm9vX3BpcHBvOmJhci1iYXo";

    public static final String VALUE = "EPFL | École polytechnique fédérale de Lausanne: home";
    public static final String ENCODED_VALUE = "CE7A46462067A4959A06E3CC010761BD|RVBGTCB8IMOJY29sZSBwb2x5dGVjaG5pcXVlIGbDqWTDqXJhbGUgZGUgTGF1c2FubmU6IGhvbWU";

    public static final String ENCODED_VALUES = "C1464EC8021E90955A3494D72A4D43E3|U3RpZyBUw7hmdGluZw,32CA428ED8CF01AFC14383134BCF3135|SmVzcGVyIEdyw7hua2rDpnI";
    public static final String STIG_TOFTING = "Stig Tøfting";
    public static final String JESPER_GRONKJAER = "Jesper Grønkjær";

    @Test
    public void decodeValue() throws Exception {
        String s = MetadataEncodingUtils.decodeValue(ENCODED_VALUE);
        assertEquals(VALUE, s);
    }

    @Test
    public void decodeValues() throws Exception {
        List<String> s = MetadataEncodingUtils.decodeValues(ENCODED_VALUES);
        assertEquals(STIG_TOFTING, s.get(0));
        assertEquals(JESPER_GRONKJAER, s.get(1));
    }

    @Test
    public void encodeKey() throws Exception {
        String encodedKey = MetadataEncodingUtils.encodeKey(KEY);
        assertEquals(ENCODED_KEY, encodedKey);
    }

    @Test
    public void decodeKey() throws Exception {
        String encodedKey = MetadataEncodingUtils.decodeKey(ENCODED_KEY);
        assertEquals(KEY, encodedKey);
    }

    @Test
    public void encodeValue() throws Exception {
        String s = MetadataEncodingUtils.encodeValue(VALUE);
        assertEquals(ENCODED_VALUE, s);
    }

    @Test
    public void testEncodeValues() {
        String s = MetadataEncodingUtils.encodeValues(Arrays.asList(STIG_TOFTING, JESPER_GRONKJAER));
        assertEquals(ENCODED_VALUES, s);
    }

}