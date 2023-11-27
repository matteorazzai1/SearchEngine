package it.unipi.mircv;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class PreprocesserTests {
    @Test
    //test the parser
    public void testParsing(){
        String input = "! hi?(HOME)[ok]{top}, no way;." +
                "and You:'try+6-8\"*program/programmer,lawyer|\\_#<>%" +
                "<div>" +
                "  <ul>" +
                "    <li>Bobby</li>" +
                "    <li>Hadz</li>" +
                "    <li>Com</li>" +
                "  </ul>" +
                "</div>";
        List<String> expected = List.of("hi", "home", "ok", "top", "way", "try", "6", "8", "program", "programmer", "lawyer", "bobby", "hadz", "com");
        List<String> output = Preprocesser.parse(input);
        assertTrue(output.equals(expected));
    }

    @Test
    //test the stemmer
    public void testStemming(){
        List<String> input = List.of("waiting", "automation");
        List<String> expected = List.of("wait", "autom");
        List<String> output = Preprocesser.stem(input);
        assertTrue(output.equals(expected));
    }
}
