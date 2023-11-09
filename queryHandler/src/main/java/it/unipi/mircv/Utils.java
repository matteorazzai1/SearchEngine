package it.unipi.mircv;
import java.util.HashMap;
import java.util.LinkedList;

import static java.lang.Float.POSITIVE_INFINITY;

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
        int minID = (int) POSITIVE_INFINITY;
        for (PostingList p : index){
            if(positions.get(p.getTerm()) < p.getPostings().size()){
                minID = Integer.min(minID, p.getPostings().get(positions.get(p.getTerm())).getDocId());
            }
        }
        return minID;
    }


}
