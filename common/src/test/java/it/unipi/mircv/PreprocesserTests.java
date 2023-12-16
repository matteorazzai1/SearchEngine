package it.unipi.mircv;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class PreprocesserTests {
    @Test
    //test the parsing and the preprocessing
    public void testParsing(){
        //parsing test
        String input = "! hi?(HOME)[ok]{top}, no way;." +
                "and You:'try+6-8\"*program/programmer,lawyer|\\_#<>%" +
                "http://www.google.com "+
                "<div>" +
                "  <ul>" +
                "    <li>Bobby</li>" +
                "    <li>Hadz</li>" +
                "    <li>Com</li>" +
                "  </ul>" +
                "</div>"; //input string with punctuation, url and html tags
        //expected output
        List<String> expected = List.of("hi", "home", "ok", "top", "way", "try", "6", "8", "program", "programmer", "lawyer", "bobby", "hadz", "com");
        List<String> output = Preprocesser.parse(input); //process and parse the input string
        assertTrue(output.equals(expected)); //check if the output is equal to the expected output
    }

    @Test
    //test the stemmer
    public void testStemming(){
        List<String> input = List.of("waiting", "automation"); //input list of words
        List<String> expected = List.of("wait", "autom"); //expected output
        List<String> output = Preprocesser.stem(input); //stem the input list
        assertTrue(output.equals(expected)); //check if the output is equal to the expected output
    }

    @Test
    //test the preprocessing
    public void testPreprocess(){
        //preprocess CLI query test
        String input = "Hi, do you know what time it is?"; //input string
        String expected = "hi know time "; //expected output
        String output = Preprocesser.processCLIQuery(input); //preprocess the input string
        assertTrue(output.equals(expected)); //check if the output is equal to the expected output

        //preprocess document test
        String input2 = "0"+'\t'+"Hi, do you know what time it is?"; //input string with docNo
        String expected2 = "0"+'\t'+"hi know time "; //expected output
        String output2 = Preprocesser.process(input2); //preprocess the input string
        assertTrue(output2.equals(expected2)); //check if the output is equal to the expected output
    }
}
