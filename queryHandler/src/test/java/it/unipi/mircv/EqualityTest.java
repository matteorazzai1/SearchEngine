package it.unipi.mircv;

import it.unipi.mircv.baseStructure.DocumentIndex;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class EqualityTest {
    @Test
    public void testEquality() throws IOException {
        //SPIMI.performSpimi(false);
        //Merger.performMerging(false);
        DocumentIndex.getInstance().loadCollectionStats();
        DocumentIndex.getInstance().readFromFile();
        String[] queries = {"orange yesterday", "sun summer protect protect"};
        for(int i=0; i< queries.length; i++){
            String query = Preprocesser.processCLIQuery(queries[i]);
            LinkedList<Map.Entry<Integer, Double>> resultsDAATTFIDF = Ranking.DAATDisjunctive(query, 5, false);
            LinkedList<Map.Entry<Integer, Double>> resultsMaxScoreTFIDF = MaxScore.maxScoreQuery(query, 5, false);
            assertTrue(resultsDAATTFIDF.containsAll(resultsMaxScoreTFIDF) && resultsMaxScoreTFIDF.containsAll(resultsDAATTFIDF));
            LinkedList<Map.Entry<Integer, Double>> resultsDAATBM25 = Ranking.DAATDisjunctive(query, 5, true);
            LinkedList<Map.Entry<Integer, Double>> resultsMaxScoreBM25 = MaxScore.maxScoreQuery(query, 5, true);
            assertTrue(resultsDAATBM25.containsAll(resultsMaxScoreBM25) && resultsMaxScoreBM25.containsAll(resultsDAATBM25));
        }

    }
}
