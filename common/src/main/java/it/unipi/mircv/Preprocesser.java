package it.unipi.mircv;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import ca.rmen.porterstemmer.PorterStemmer;
public class Preprocesser {

    private static List<String> stopwords = new ArrayList<>();
    private static List<String> punctuation = new ArrayList<>();

    public static List<String> parse(String text) {
        if(stopwords.isEmpty()) {
            stopwords.add("a");
            stopwords.add("an");
            stopwords.add("and");
            stopwords.add("are");
            stopwords.add("as");
            stopwords.add("at");
            stopwords.add("be");
            stopwords.add("but");
            stopwords.add("by");
            stopwords.add("for");
            stopwords.add("if");
            stopwords.add("in");
            stopwords.add("into");
            stopwords.add("is");
            stopwords.add("it");
            stopwords.add("no");
            stopwords.add("not");
            stopwords.add("of");
            stopwords.add("on");
            stopwords.add("or");
            stopwords.add("such");
            stopwords.add("that");
            stopwords.add("the");
            stopwords.add("their");
            stopwords.add("then");
            stopwords.add("there");
            stopwords.add("these");
            stopwords.add("they");
            stopwords.add("this");
            stopwords.add("to");
            stopwords.add("was");
            stopwords.add("will");
            stopwords.add("with");
        }
        if (punctuation.isEmpty()) {
            punctuation.add("!");
            punctuation.add("?");
            punctuation.add("(");
            punctuation.add(")");
            punctuation.add("[");
            punctuation.add("]");
            punctuation.add("{");
            punctuation.add("}");
            punctuation.add(",");
            punctuation.add(";");
            punctuation.add(".");
            punctuation.add(":");
            punctuation.add("'");
            punctuation.add("“");
            punctuation.add("”");
            punctuation.add("’");
            punctuation.add("+");
            punctuation.add("-");
            punctuation.add("*");
            punctuation.add("/");
            punctuation.add("|");
            punctuation.add("\"");
            punctuation.add("\\");
            punctuation.add("_");
            punctuation.add("#");
            punctuation.add("<");
            punctuation.add(">");
            punctuation.add("%");
            punctuation.add("=");
            punctuation.add("^");
            punctuation.add("$");
        }

        Pattern htmlPattern = Pattern.compile("<.*?>");
        Matcher matcher = htmlPattern.matcher(text);
        text = matcher.replaceAll(" ");
        for (String element : punctuation) {
            text = text.replace(element, " ");
        }
        String[] termArray = text.split("\\s+");
        List<String> termList = new ArrayList<>();
        for (String term : termArray) {
            if(!stopwords.contains(term)) {
                term = term.toLowerCase();
                termList.add(term);
            }
        }

        return termList;
    }

    public static List<String> stem(List<String> terms) {
        List<String> newTerms = new ArrayList<>();
        PorterStemmer stemmer = new PorterStemmer();
        for (String term : terms) {
            newTerms.add(stemmer.stemWord(term));
        }
        return newTerms;
    }

    public static String process(String row) {
        List<String>terms = parse(row);
        terms=stem(terms);
        String termsString = terms.get(0) + "\t";
        terms.remove(0);
        for(String term : terms){
            termsString += term + " ";
        }
        return termsString;
    }
}
