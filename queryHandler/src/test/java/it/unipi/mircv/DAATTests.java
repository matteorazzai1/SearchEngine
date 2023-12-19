package it.unipi.mircv;

import it.unipi.mircv.baseStructure.DocumentIndex;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

import static it.unipi.mircv.DAAT.DAATConjunctive;
import static it.unipi.mircv.DAAT.DAATDisjunctive;
import static org.junit.Assert.assertTrue;

public class DAATTests {

    @Test
    //test the disjunctive DAAT
    public void testDisjunctiveDAAT() throws IOException {
        //SPIMI.performSpimi(false); //perform the SPIMI algorithm
        //Merger.performMerging(false); //perform the merging algorithm
        DocumentIndex.getInstance().loadCollectionStats(); //load the collection stats
        DocumentIndex.getInstance().readFromFile(); //read the document index from file
        String[] queries = {"orange yesterday", "sun summer protect protect"}; //queries to be tested
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedTFIDF = setupDisTFIDF(); //expected results for TFIDF
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedBM25 = setupDisBM25(); //expected results for BM25
        //test the disjunctive DAAT for TFIDF
        for(int i = 0; i < queries.length; i++){ //for each query
            String query = Preprocesser.processCLIQuery(queries[i]); //process the query
            //perform the disjunctive DAAT
            LinkedList<Map.Entry<Integer, Double>> results = DAATDisjunctive(query, 5, false);
            for(int j = 0; j < results.size(); j++){ //for each returned document
                Map.Entry<Integer, Double> result = results.get(j); //get the j-th document
                Map.Entry<Integer, Double> exp = expectedTFIDF.get(i).get(j); //get the j-th expected document
                assertTrue(Objects.equals(result.getKey(), exp.getKey())); //check if the docIDs are equal
                //check if the scores are equal
                assertTrue(Math.round(result.getValue()*Math.pow(10, 15))==(Math.round(exp.getValue()*Math.pow(10, 15))));
            }
        }
        //test the disjunctive DAAT for BM25
        for(int i = 0; i < queries.length; i++){ //for each query
            String query = Preprocesser.processCLIQuery(queries[i]); //process the query
            //perform the disjunctive DAAT
            LinkedList<Map.Entry<Integer, Double>> results = DAATDisjunctive(query, 5, true);
            for(int j = 0; j < results.size(); j++){ //for each returned document
                Map.Entry<Integer, Double> result = results.get(j); //get the j-th document
                Map.Entry<Integer, Double> exp = expectedBM25.get(i).get(j); //get the j-th expected document
                assertTrue(Objects.equals(result.getKey(), exp.getKey())); //check if the docIDs are equal
                //check if the scores are equal
                assertTrue(Math.round(result.getValue()*Math.pow(10, 15))==(Math.round(exp.getValue()*Math.pow(10, 15))));
            }
        }
    }

    @Test
    //test the conjunctive DAAT
    public void testConjunctiveDAAT() throws IOException {
        //SPIMI.performSpimi(false); //perform the SPIMI algorithm
        //Merger.performMerging(false); //perform the merging algorithm
        DocumentIndex.getInstance().loadCollectionStats(); //load the collection stats
        DocumentIndex.getInstance().readFromFile(); //read the document index from file
        String[] queries = {"orange yesterday", "sun summer protect protect"}; //queries to be tested
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedTFIDF = setupConTFIDF(); //expected results for TFIDF
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedBM25 = setupConBM25(); //expected results for BM25
        //test the conjunctive DAAT for TFIDF
        for(int i = 0; i < queries.length; i++){ //for each query
            String query = Preprocesser.processCLIQuery(queries[i]); //process the query
            //perform the conjunctive DAAT
            LinkedList<Map.Entry<Integer, Double>> results = DAATConjunctive(query, 5, false);
            for(int j = 0; j < results.size(); j++){ //for each returned document
                Map.Entry<Integer, Double> result = results.get(j); //get the j-th document
                Map.Entry<Integer, Double> exp = expectedTFIDF.get(i).get(j); //get the j-th expected document
                assertTrue(Objects.equals(result.getKey(), exp.getKey())); //check if the docIDs are equal
                //check if the scores are equal
                assertTrue(Math.round(result.getValue()*Math.pow(10, 15))==(Math.round(exp.getValue()*Math.pow(10, 15))));
            }
        }
        //test the conjunctive DAAT for BM25
        for(int i = 0; i < queries.length; i++){ //for each query
            String query = Preprocesser.processCLIQuery(queries[i]); //process the query
            //perform the conjunctive DAAT
            LinkedList<Map.Entry<Integer, Double>> results = DAATConjunctive(query, 5, true);
            for(int j = 0; j < results.size(); j++){ //for each returned document
                Map.Entry<Integer, Double> result = results.get(j); //get the j-th document
                Map.Entry<Integer, Double> exp = expectedBM25.get(i).get(j); //get the j-th expected document
                assertTrue(Objects.equals(result.getKey(), exp.getKey())); //check if the docIDs are equal
                //check if the scores are equal
                assertTrue(Math.round(result.getValue()*Math.pow(10, 15))==(Math.round(exp.getValue()*Math.pow(10, 15))));
            }
        }
    }



    //setup the expected results for disjunctive TFIDF
    private LinkedList<LinkedList<Map.Entry<Integer, Double>>> setupDisTFIDF(){
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedTFIDF = new LinkedList<>(); //expected results
        LinkedList<Map.Entry<Integer, Double>> expectedTFIDF1 = new LinkedList<>(); //expected results for the first query
        expectedTFIDF1.add(Map.entry(8, 1.6989700043360189));
        expectedTFIDF1.add(Map.entry(5, 0.6989700043360189));
        expectedTFIDF.add(expectedTFIDF1); //add the expected results for the first query
        LinkedList<Map.Entry<Integer, Double>> expectedTFIDF2 = new LinkedList<>(); //expected results for the second query
        expectedTFIDF2.add(Map.entry(6, 3.3979400086720376));
        expectedTFIDF2.add(Map.entry(1, 0.6989700043360189));
        expectedTFIDF2.add(Map.entry(7, 0.6989700043360189));
        expectedTFIDF.add(expectedTFIDF2); //add the expected results for the second query
        return expectedTFIDF;
    }

    //setup the expected results for conjunctive TFIDF
    private LinkedList<LinkedList<Map.Entry<Integer, Double>>> setupConTFIDF(){
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedTFIDF = new LinkedList<>(); //expected results
        LinkedList<Map.Entry<Integer, Double>> expectedTFIDF1 = new LinkedList<>(); //expected results for the first query
        expectedTFIDF1.add(Map.entry(8, 1.6989700043360189));
        expectedTFIDF.add(expectedTFIDF1); //add the expected results for the first query
        LinkedList<Map.Entry<Integer, Double>> expectedTFIDF2 = new LinkedList<>(); //expected results for the second query
        expectedTFIDF2.add(Map.entry(6, 3.3979400086720376));
        expectedTFIDF.add(expectedTFIDF2); //add the expected results for the second query
        return expectedTFIDF;
    }

    //setup the expected results for disjunctive BM25
    private LinkedList<LinkedList<Map.Entry<Integer, Double>>> setupDisBM25(){
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedBM25 = new LinkedList<>(); //expected results
        LinkedList<Map.Entry<Integer, Double>> expectedBM25_1 = new LinkedList<>(); //expected results for the first query
        expectedBM25_1.add(Map.entry(8, 0.6556650864191155));
        expectedBM25_1.add(Map.entry(5, 0.3056057921469152));
        expectedBM25.add(expectedBM25_1); //add the expected results for the first query
        LinkedList<Map.Entry<Integer, Double>> expectedBM25_2 = new LinkedList<>(); //expected results for the second query
        expectedBM25_2.add(Map.entry(6, 1.1736175525868415));
        expectedBM25_2.add(Map.entry(1, 0.3056057921469152));
        expectedBM25_2.add(Map.entry(7, 0.21847425689911462));
        expectedBM25.add(expectedBM25_2); //add the expected results for the second query
        return expectedBM25;
    }

    //setup the expected results for conjunctive BM25
    private LinkedList<LinkedList<Map.Entry<Integer, Double>>> setupConBM25(){
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedBM25 = new LinkedList<>(); //expected results
        LinkedList<Map.Entry<Integer, Double>> expectedBM25_1 = new LinkedList<>(); //expected results for the first query
        expectedBM25_1.add(Map.entry(8, 0.6556650864191155));
        expectedBM25.add(expectedBM25_1); //add the expected results for the first query
        LinkedList<Map.Entry<Integer, Double>> expectedBM25_2 = new LinkedList<>(); //expected results for the second query
        expectedBM25_2.add(Map.entry(6, 1.1736175525868415));
        expectedBM25.add(expectedBM25_2); //add the expected results for the second query
        return expectedBM25;
    }
}
