package no.ssb.dc.collection.api.source;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class AsciiTest {

    @Test
    void name() {
        List<Character> illegalChars = new ArrayList<>();
        IntStream.range(33, 47).forEach(i -> illegalChars.add((char) i));
        IntStream.range(58, 64).forEach(i -> illegalChars.add((char) i));
        IntStream.range(91, 96).forEach(i -> illegalChars.add((char) i));
        IntStream.range(123, 126).forEach(i -> illegalChars.add((char) i));
        illegalChars.forEach(c -> System.out.printf("%s%n", c));
    }
}
