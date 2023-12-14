package it.unipi.mircv;
import it.unipi.mircv.baseStructure.DocumentIndex;
import it.unipi.mircv.baseStructure.PostingList;
import it.unipi.mircv.baseStructure.SkippingBlock;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;

import static it.unipi.mircv.Constants.b;
import static it.unipi.mircv.Constants.k1;
import static it.unipi.mircv.baseStructure.SkippingBlock.readSkippingBlocks;
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

    public static int minDocID(ArrayList<PostingList> index, Map<String, AbstractMap.SimpleEntry<Integer, Integer>> positions){ //positions are indexes, not docids
        int minID = Integer.MAX_VALUE;
        for (PostingList p : index){
            if(positions.get(p.getTerm()).getKey() < p.getPostings().size()){
                minID = Integer.min(minID, p.getPostings().get(positions.get(p.getTerm()).getKey()).getDocId());
            }
        }
        return minID;
    }

    public static AbstractMap.SimpleEntry<Integer, Integer> moveToNext(PostingList p, int position,
                                                                       int block, int numBlocks, long descriptorOffset, FileChannel blockChannel) throws IOException {

        if(position + 1 < p.getPostingsLength()){
            return new AbstractMap.SimpleEntry<>(position + 1, block);
        }
        else if(block + 1 < numBlocks){
            p.rewritePostings(readSkippingBlocks(descriptorOffset + ((long) (block + 1) * SkippingBlock.getEntrySize()), blockChannel).retrieveBlock());
            return new AbstractMap.SimpleEntry<>(0, block + 1);
        }
        else{
            return null;
        }

    }


    private static double tfIDF(int termQueryFrequency, int termDocFrequency, double termIDF){
        return (termQueryFrequency * ((1 + log10(termDocFrequency)) * (termIDF)));
    }

    private static double BM25 (int termQueryFrequency, int termDocFrequency, double termIDF, int docLen){
        return termQueryFrequency * ((termDocFrequency/((k1*((1- b) + (b*docLen/ DocumentIndex.getInstance().getAVDL())))+termDocFrequency)) * (termIDF));
    }

    public static double scoringFunction(boolean isBM25, int termQueryFrequency, int termDocFrequency, double termIDF, int docLen){
        return(isBM25 ? BM25(termQueryFrequency, termDocFrequency, termIDF, docLen) : tfIDF(termQueryFrequency, termDocFrequency, termIDF));
    }


}
