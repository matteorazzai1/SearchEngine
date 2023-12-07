package it.unipi.mircv;
import it.unipi.mircv.baseStructure.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;

public class MergerTests {
    @Test
    //test the merger
    public void testMerger() throws IOException {
        ArrayList<String> filePaths = new ArrayList<>();
        filePaths.add("src/test/data/intermediateIndex1.txt");
        filePaths.add("src/test/data/intermediateIndex2.txt");
        ArrayList<ArrayList<int[]>> expected=CreateIndex(); //create expected index
        int[] docLens = new int[]{6, 13, 2, 4, 3, 11, 4, 3, 4, 4}; //number of tokens in each document
        Object[] results = UnitTestMerger.performUnitTestMerger(filePaths, docLens);
        Lexicon lexiconExpected = CreateLexicon(); //create expected lexicon
        ArrayList<ArrayList<int[]>> index = (ArrayList<ArrayList<int[]>>) results[0];
        Lexicon lexicon = (Lexicon) results[1];
        //final index test
        for (int i = 0; i < index.size(); i++) {
            assertTrue(Arrays.equals(index.get(i).get(0), expected.get(i).get(0)));
            assertTrue(Arrays.equals(index.get(i).get(1), expected.get(i).get(1)));
        }
        //lexicon test
        for (String term : lexiconExpected.getLexicon().keySet()) {
            assertTrue(lexiconExpected.getLexicon().get(term).toString().equals(lexicon.getLexicon().get(term).toString()));
        }
        DocumentIndex.resetInstance();
    }

    private ArrayList<ArrayList<int[]>> CreateIndex(){
        ArrayList<ArrayList<int[]>> expected = new ArrayList<>();
        ArrayList<int[]> expected1 = new ArrayList<>();
        expected1.add(new int[]{1, 3});
        expected1.add(new int[]{3, 2});
        expected.add(expected1);
        ArrayList<int[]> expected2 = new ArrayList<>();
        expected2.add(new int[]{6});
        expected2.add(new int[]{3});
        expected.add(expected2);
        ArrayList<int[]> expected3 = new ArrayList<>();
        expected3.add(new int[]{1, 2, 4, 6, 8});
        expected3.add(new int[]{2, 1, 1, 4, 2});
        expected.add(expected3);
        ArrayList<int[]> expected4 = new ArrayList<>();
        expected4.add(new int[]{2, 4});
        expected4.add(new int[]{4, 1});
        expected.add(expected4);
        ArrayList<int[]> expected5 = new ArrayList<>();
        expected5.add(new int[]{7, 9});
        expected5.add(new int[]{1, 3});
        expected.add(expected5);
        ArrayList<int[]> expected6 = new ArrayList<>();
        expected6.add(new int[]{1, 2, 5, 7, 8, 10});
        expected6.add(new int[]{1, 3, 1, 3, 1, 2});
        expected.add(expected6);
        ArrayList<int[]> expected7 = new ArrayList<>();
        expected7.add(new int[]{2, 4, 5, 6, 9, 10});
        expected7.add(new int[]{5, 2, 2, 4, 1, 2});
        expected.add(expected7);
        return expected;
    }

    private Lexicon CreateLexicon(){
        Lexicon lexiconExpected = new Lexicon();
        HashMap<String, LexiconEntry> lexiconEntries = new HashMap<>();
        LexiconEntry lexiconEntry1 = new LexiconEntry("cloud");
        lexiconEntry1.setDf(2);
        lexiconEntry1.setIdf(2);
        lexiconEntry1.setTermCollFreq(5);
        lexiconEntry1.setMaxTf(3);
        lexiconEntry1.setMaxTfidf(3);
        lexiconEntry1.computeMaxBM25(new PostingList("cloud", new ArrayList<Posting>() {{
            add(new Posting(1, 3));
            add(new Posting(2, 2));
        }}));
        lexiconEntries.put("cloud", lexiconEntry1);
        LexiconEntry lexiconEntry2 = new LexiconEntry("dish");
        lexiconEntry2.setDf(1);
        lexiconEntry2.setIdf(1);
        lexiconEntry2.setTermCollFreq(3);
        lexiconEntry2.setMaxTf(3);
        lexiconEntry2.setMaxTfidf(3);
        lexiconEntry2.computeMaxBM25(new PostingList("dish", new ArrayList<Posting>() {{
            add(new Posting(6, 3));
        }}));
        lexiconEntries.put("dish", lexiconEntry2);
        LexiconEntry lexiconEntry3 = new LexiconEntry("door");
        lexiconEntry3.setDf(5);
        lexiconEntry3.setIdf(5);
        lexiconEntry3.setTermCollFreq(10);
        lexiconEntry3.setMaxTf(4);
        lexiconEntry3.setMaxTfidf(4);
        lexiconEntry3.computeMaxBM25(new PostingList("door", new ArrayList<Posting>() {{
            add(new Posting(1, 2));
            add(new Posting(2, 1));
            add(new Posting(4, 1));
            add(new Posting(6, 4));
            add(new Posting(8, 2));
        }}));
        lexiconEntries.put("door", lexiconEntry3);
        LexiconEntry lexiconEntry4 = new LexiconEntry("feet");
        lexiconEntry4.setDf(2);
        lexiconEntry4.setIdf(2);
        lexiconEntry4.setTermCollFreq(5);
        lexiconEntry4.setMaxTf(4);
        lexiconEntry4.setMaxTfidf(4);
        lexiconEntry4.computeMaxBM25(new PostingList("feet", new ArrayList<Posting>() {{
            add(new Posting(2, 4));
            add(new Posting(4, 1));
        }}));
        lexiconEntries.put("feet", lexiconEntry4);
        LexiconEntry lexiconEntry5 = new LexiconEntry("glass");
        lexiconEntry5.setDf(2);
        lexiconEntry5.setIdf(2);
        lexiconEntry5.setTermCollFreq(4);
        lexiconEntry5.setMaxTf(3);
        lexiconEntry5.setMaxTfidf(3);
        lexiconEntry5.computeMaxBM25(new PostingList("glass", new ArrayList<Posting>() {{
            add(new Posting(7, 1));
            add(new Posting(9, 3));
        }}));
        lexiconEntries.put("glass", lexiconEntry5);
        LexiconEntry lexiconEntry6 = new LexiconEntry("table");
        lexiconEntry6.setDf(6);
        lexiconEntry6.setIdf(6);
        lexiconEntry6.setTermCollFreq(11);
        lexiconEntry6.setMaxTf(3);
        lexiconEntry6.setMaxTfidf(3);
        lexiconEntry6.computeMaxBM25(new PostingList("table", new ArrayList<Posting>() {{
            add(new Posting(1, 1));
            add(new Posting(2, 3));
            add(new Posting(5, 1));
            add(new Posting(7, 3));
            add(new Posting(8, 1));
            add(new Posting(10, 2));
        }}));
        lexiconEntries.put("table", lexiconEntry6);
        LexiconEntry lexiconEntry7 = new LexiconEntry("window");
        lexiconEntry7.setDf(6);
        lexiconEntry7.setIdf(6);
        lexiconEntry7.setTermCollFreq(16);
        lexiconEntry7.setMaxTf(5);
        lexiconEntry7.setMaxTfidf(5);
        lexiconEntry7.computeMaxBM25(new PostingList("window", new ArrayList<Posting>() {{
            add(new Posting(2, 5));
            add(new Posting(4, 2));
            add(new Posting(5, 2));
            add(new Posting(6, 4));
            add(new Posting(9, 1));
            add(new Posting(10, 2));
        }}));
        lexiconEntries.put("window", lexiconEntry7);
        lexiconExpected.setLexicon(lexiconEntries);
        return lexiconExpected;
    }
}
