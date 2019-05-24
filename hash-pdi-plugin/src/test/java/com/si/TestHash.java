package com.si;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.junit.jupiter.api.Test;


public class TestHash {

    @Test
    public void shouldHashTheTextWithMurmurhash3(){
        String text =  "I am a long piece of text. This one should hash pretty well, don't ya know y'all.";
        String text2 = "I am a long piece of text. This one should hash pretty well, don't ya know y'all. Not Same.";
        HashFunction hfunc = Hashing.murmur3_128(1073741823);
        HashCode hcode = hfunc.hashBytes(text.getBytes());
        Long hc = hcode.asLong();
        HashCode hcode2 = hfunc.hashBytes(text2.getBytes());
        Long hc2 = hcode2.asLong();
        assert(hc != 0);
        assert(hc2 != 0);
        assert(hc != hc2);
    }
}
