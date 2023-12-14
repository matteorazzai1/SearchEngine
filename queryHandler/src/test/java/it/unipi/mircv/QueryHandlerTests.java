package it.unipi.mircv;

import it.unipi.mircv.baseStructure.DocumentIndex;
import it.unipi.mircv.baseStructure.PostingList;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

import static it.unipi.mircv.Constants.PATH_TO_COLLECTION;
import static it.unipi.mircv.Ranking.DAATDisjunctive;
import static org.junit.Assert.assertTrue;

public class QueryHandlerTests {

    @Test
    public void testDisjunctiveDAAT() throws IOException {
        SPIMI.performSpimi(false);
        Merger.performMerging(false);
        DocumentIndex.getInstance().loadCollectionStats();
        DocumentIndex.getInstance().readFromFile();
        String[] queries = {"orange yesterday", "sun summer protect protect"};
        LinkedList<LinkedList<Map.Entry<Integer, Double>>> expectedTFIDF = setupTFIDF();
        for(int i = 0; i < queries.length; i++){
            String query = Preprocesser.processCLIQuery(queries[i]);
            LinkedList<Map.Entry<Integer, Double>> results = DAATDisjunctive(query, false, 5);
            for(int j = 0; j < results.size(); j++){
                Map.Entry<Integer, Double> result = results.get(j);
                Map.Entry<Integer, Double> exp = expectedTFIDF.get(i).get(j);
                assertTrue(Objects.equals(result.getKey(), exp.getKey()));
                assertTrue(Math.round(result.getValue()*Math.pow(10, 15))==(Math.round(exp.getValue()*Math.pow(10, 15))));
            }
        }
    }

    private LinkedList<LinkedList<Map.Entry<Integer, Double>>> setupTFIDF(){
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

}
