package it.unipi.mircv;

import it.unipi.mircv.baseStructure.DocumentIndex;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class EqualityTest {
    @Test
    //test the equality of the results of the two ranking algorithms
    public void testEquality() throws IOException {
        //SPIMI.performSpimi(false); //perform the SPIMI algorithm
        //Merger.performMerging(false); //perform the merging algorithm
        DocumentIndex.getInstance().loadCollectionStats(); //load the collection stats
        DocumentIndex.getInstance().readFromFile(); //read the document index from file
        String[] queries = {"orange yesterday", "sun summer protect protect"}; //queries to be tested
        for(int i=0; i< queries.length; i++){ //for each query
            String query = Preprocesser.processCLIQuery(queries[i]); //process the query
            //perform the two ranking algorithms for TFIDF
            LinkedList<Map.Entry<Integer, Double>> resultsDAATTFIDF = Ranking.DAATDisjunctive(query, 5, false);
            LinkedList<Map.Entry<Integer, Double>> resultsMaxScoreTFIDF = MaxScore.maxScoreQuery(query, 5, false);
            //check if the results are equal
            assertTrue(resultsDAATTFIDF.containsAll(resultsMaxScoreTFIDF) && resultsMaxScoreTFIDF.containsAll(resultsDAATTFIDF));
            //perform the two ranking algorithms for BM25
            LinkedList<Map.Entry<Integer, Double>> resultsDAATBM25 = Ranking.DAATDisjunctive(query, 5, true);
            LinkedList<Map.Entry<Integer, Double>> resultsMaxScoreBM25 = MaxScore.maxScoreQuery(query, 5, true);
            //check if the results are equal
            assertTrue(resultsDAATBM25.containsAll(resultsMaxScoreBM25) && resultsMaxScoreBM25.containsAll(resultsDAATBM25));
        }
    }
}
