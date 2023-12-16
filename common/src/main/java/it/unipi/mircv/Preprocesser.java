package it.unipi.mircv;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import ca.rmen.porterstemmer.PorterStemmer;
public class Preprocesser {

    //a list of stopwords that will be removed from the text
    private static List<String> stopwords = Arrays.asList(
            "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are",
            "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both",
            "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't",
            "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't",
            "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here",
            "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm",
            "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "might", "more",
            "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or",
            "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
            "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's",
            "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they",
            "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under",
            "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't",
            "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom",
            "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've",
            "your", "yours", "yourself", "yourselves");

    // list of punctuation
    private static List<String> punctuation = Arrays.asList(
            "!", "?", "(", ")", "[", "]", "{", "}", ",", ";", ".", ":", "'", "“", "”", "’",
            "+", "-", "*", "/", "|", "\"", "\\", "_", "#", "<", ">", "%", "=", "^", "$", "&");

    // a method to process and parse the text
    public static List<String> parse(String text) {
        text = text.replaceAll("<.*?>", " "); //remove html tags
        text = text.replaceAll("https?://\\S+\\s?", ""); //remove urls
        //remove punctuation
        for (String element : punctuation) {
            text = text.replace(element, " ");
        }
        //remove initial space
        while(text.startsWith(" ")){
            text = text.substring(1);
        }
        String[] termArray = text.split("\\s+"); //split the text into terms
        List<String> termList = new ArrayList<>();
        for (String term : termArray) { //for each term
            term = term.toLowerCase(); //convert the term to lowercase
            //add only the terms that are not stopwords and do not contain special characters
            if(!stopwords.contains(term) && !term.matches("(?s).*[^a-zA-Z0-9](?s).*")) {
                termList.add(term);
            }
        }

        return termList;
    }

    //a method to stem the terms using the Porter Stemmer
    public static List<String> stem(List<String> terms) {
        List<String> newTerms = new ArrayList<>(); //list for the stemmed terms
        PorterStemmer stemmer = new PorterStemmer(); //create a new stemmer
        for (String term : terms) {
            newTerms.add(stemmer.stemWord(term)); //stem the term and add it to the list
        }
        return newTerms;
    }

    //a method to process the CLIquery (which has not the docID)
    public static String processCLIQuery(String row) {
        List<String>terms = parse(row); //parse the query
        terms=stem(terms); //stem the query
        String termsString = ""; //string for the query
        for(String term : terms){
            termsString += term + " "; //add the terms to the string
        }
        return termsString;
    }

    //a method to process the row of the collection (which has the docID)
    public static String process(String row) {
        List<String>terms = parse(row); //parse the row
        terms=stem(terms); //stem the row
        String termsString = terms.get(0) + "\t"; //string for the row
        terms.remove(0); //remove the docID
        for(String term : terms){ //for each term
            termsString += term + " "; //add the terms to the string
        }
        return termsString;
    }

}
