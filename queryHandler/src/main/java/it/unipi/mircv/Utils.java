package it.unipi.mircv;
import it.unipi.mircv.baseStructure.DocumentIndex;
import it.unipi.mircv.baseStructure.PostingList;

import java.util.HashMap;
import java.util.LinkedList;

import static java.lang.Math.log10;

public class Utils {

    public static HashMap<String, Integer> queryToDict(String query){
        HashMap<String, Integer> processedQuery = new HashMap<>();
        for (String s : query.split(" ")){
            if (processedQuery.containsKey(s)){
                processedQuery.put(s, processedQuery.get(s) + 1);
            }
            else{
                processedQuery.put(s, 1);
            }
        }
        return processedQuery;
    }

    public static int minDocID(LinkedList<PostingList> index, HashMap<String, Integer> positions){ //positions are indexes, not docids
        int minID = Integer.MAX_VALUE;
        for (PostingList p : index){
            if(positions.get(p.getTerm()) < p.getPostings().size()){
                minID = Integer.min(minID, p.getPostings().get(positions.get(p.getTerm())).getDocId());
            }
        }
        return minID;
    }

    private static double tfIDF(int termQueryFrequency, int termDocFrequency, double termIDF){
        return (termQueryFrequency * ((1 + log10(termDocFrequency)) * (termIDF)));
    }

    private static double BM25 (int termQueryFrequency, int termDocFrequency, double termIDF, int docLen){
        return termQueryFrequency * ((termDocFrequency/((Constants.k1*((1-Constants.b) + (Constants.b*docLen/ DocumentIndex.getInstance().getAVDL())))+termDocFrequency)) * (termIDF));
    }

    public static double scoringFunction(boolean isBM25, int termQueryFrequency, int termDocFrequency, double termIDF, int docLen){
        return(isBM25 ? BM25(termQueryFrequency, termDocFrequency, termIDF, docLen) : tfIDF(termQueryFrequency, termDocFrequency, termIDF));
    }


}
