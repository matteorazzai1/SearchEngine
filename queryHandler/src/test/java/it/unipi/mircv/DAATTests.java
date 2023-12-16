package it.unipi.mircv;

import it.unipi.mircv.baseStructure.DocumentIndex;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

import static it.unipi.mircv.Ranking.DAATConjunctive;
import static it.unipi.mircv.Ranking.DAATDisjunctive;
import static org.junit.Assert.assertTrue;

public class DAATTests {

    @Test
    public void testDisjunctiveDAAT() throws IOException {
        //SPIMI.performSpimi(false);
        //Merger.performMerging(false);
        DocumentIndex.getInstance().loadCollectionStats();
        DocumentIndex.getInstance().readFromFile();
        String[] queries = {"orange yesterday", "sun summer protect protect"};
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedTFIDF = setupDisTFIDF();
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedBM25 = setupDisBM25();
        for(int i = 0; i < queries.length; i++){
            String query = Preprocesser.processCLIQuery(queries[i]);
            LinkedList<Map.Entry<Integer, Double>> results = DAATDisjunctive(query, 5, false);
            for(int j = 0; j < results.size(); j++){
                Map.Entry<Integer, Double> result = results.get(j);
                Map.Entry<Integer, Double> exp = expectedTFIDF.get(i).get(j);
                assertTrue(Objects.equals(result.getKey(), exp.getKey()));
                assertTrue(Math.round(result.getValue()*Math.pow(10, 15))==(Math.round(exp.getValue()*Math.pow(10, 15))));
            }
        }
        for(int i = 0; i < queries.length; i++){
            String query = Preprocesser.processCLIQuery(queries[i]);
            LinkedList<Map.Entry<Integer, Double>> results = DAATDisjunctive(query, 5, true);
            for(int j = 0; j < results.size(); j++){
                Map.Entry<Integer, Double> result = results.get(j);
                Map.Entry<Integer, Double> exp = expectedBM25.get(i).get(j);
                assertTrue(Objects.equals(result.getKey(), exp.getKey()));
                assertTrue(Math.round(result.getValue()*Math.pow(10, 15))==(Math.round(exp.getValue()*Math.pow(10, 15))));
            }
        }
    }

    @Test
    public void testConjunctiveDAAT() throws IOException {
        //SPIMI.performSpimi(false);
        //Merger.performMerging(false);
        DocumentIndex.getInstance().loadCollectionStats();
        DocumentIndex.getInstance().readFromFile();
        String[] queries = {"orange yesterday", "sun summer protect protect"};
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedTFIDF = setupConTFIDF();
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedBM25 = setupConBM25();
        for(int i = 0; i < queries.length; i++){
            String query = Preprocesser.processCLIQuery(queries[i]);
            LinkedList<Map.Entry<Integer, Double>> results = DAATConjunctive(query, 5, false);
            for(int j = 0; j < results.size(); j++){
                Map.Entry<Integer, Double> result = results.get(j);
                Map.Entry<Integer, Double> exp = expectedTFIDF.get(i).get(j);
                assertTrue(Objects.equals(result.getKey(), exp.getKey()));
                assertTrue(Math.round(result.getValue()*Math.pow(10, 15))==(Math.round(exp.getValue()*Math.pow(10, 15))));
            }
        }
        for(int i = 0; i < queries.length; i++){
            String query = Preprocesser.processCLIQuery(queries[i]);
            LinkedList<Map.Entry<Integer, Double>> results = DAATConjunctive(query, 5, true);
            for(int j = 0; j < results.size(); j++){
                Map.Entry<Integer, Double> result = results.get(j);
                Map.Entry<Integer, Double> exp = expectedBM25.get(i).get(j);
                assertTrue(Objects.equals(result.getKey(), exp.getKey()));
                assertTrue(Math.round(result.getValue()*Math.pow(10, 15))==(Math.round(exp.getValue()*Math.pow(10, 15))));
            }
        }
    }



    private LinkedList<LinkedList<Map.Entry<Integer, Double>>> setupDisTFIDF(){
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedTFIDF = new LinkedList<>();
        LinkedList<Map.Entry<Integer, Double>> expectedTFIDF1 = new LinkedList<>();
        expectedTFIDF1.add(Map.entry(8, 1.6989700043360189));
        expectedTFIDF1.add(Map.entry(5, 0.6989700043360189));
        expectedTFIDF.add(expectedTFIDF1);
        LinkedList<Map.Entry<Integer, Double>> expectedTFIDF2 = new LinkedList<>();
        expectedTFIDF2.add(Map.entry(6, 3.3979400086720376));
        expectedTFIDF2.add(Map.entry(1, 0.6989700043360189));
        expectedTFIDF2.add(Map.entry(7, 0.6989700043360189));
        expectedTFIDF.add(expectedTFIDF2);
        return expectedTFIDF;
    }

    private LinkedList<LinkedList<Map.Entry<Integer, Double>>> setupConTFIDF(){
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedTFIDF = new LinkedList<>();
        LinkedList<Map.Entry<Integer, Double>> expectedTFIDF1 = new LinkedList<>();
        expectedTFIDF1.add(Map.entry(8, 1.6989700043360189));
        expectedTFIDF.add(expectedTFIDF1);
        LinkedList<Map.Entry<Integer, Double>> expectedTFIDF2 = new LinkedList<>();
        expectedTFIDF2.add(Map.entry(6, 3.3979400086720376));
        expectedTFIDF.add(expectedTFIDF2);
        return expectedTFIDF;
    }

    private LinkedList<LinkedList<Map.Entry<Integer, Double>>> setupDisBM25(){
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedBM25 = new LinkedList<>();
        LinkedList<Map.Entry<Integer, Double>> expectedBM25_1 = new LinkedList<>();
        expectedBM25_1.add(Map.entry(8, 0.6556650864191155));
        expectedBM25_1.add(Map.entry(5, 0.3056057921469152));
        expectedBM25.add(expectedBM25_1);
        LinkedList<Map.Entry<Integer, Double>> expectedBM25_2 = new LinkedList<>();
        expectedBM25_2.add(Map.entry(6, 1.1736175525868415));
        expectedBM25_2.add(Map.entry(1, 0.3056057921469152));
        expectedBM25_2.add(Map.entry(7, 0.21847425689911462));
        expectedBM25.add(expectedBM25_2);
        return expectedBM25;
    }

    private LinkedList<LinkedList<Map.Entry<Integer, Double>>> setupConBM25(){
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedBM25 = new LinkedList<>();
        LinkedList<Map.Entry<Integer, Double>> expectedBM25_1 = new LinkedList<>();
        expectedBM25_1.add(Map.entry(8, 0.6556650864191155));
        expectedBM25.add(expectedBM25_1);
        LinkedList<Map.Entry<Integer, Double>> expectedBM25_2 = new LinkedList<>();
        expectedBM25_2.add(Map.entry(6, 1.1736175525868415));
        expectedBM25.add(expectedBM25_2);
        return expectedBM25;
    }
}
