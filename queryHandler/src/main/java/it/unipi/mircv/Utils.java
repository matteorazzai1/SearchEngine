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

    /**
     * This function takes a query and returns a dictionary with the terms and their frequencies
     * @param query the string taken as input from the user and processed
     * @return a dictionary with the terms and their frequencies
     */
    public static HashMap<String, Integer> queryToDict(String query){
        HashMap<String, Integer> processedQuery = new HashMap<>();
        for (String s : query.split(" ")){
            //updating the entry if already present, otherwise creating a new one
            if (processedQuery.containsKey(s)){
                processedQuery.put(s, processedQuery.get(s) + 1);
            }
            else{
                processedQuery.put(s, 1);
            }
        }
        return processedQuery;
    }

    /**
     * Function to find the minimum pointed docID among the current postings
     * @param index the index from which we want to retrieve minDocID
     * @param positions dictionary keeping track of the current position and block for each posting list in the index
     * @return minDocID among all the postingLists
     */
    public static int minDocID(ArrayList<PostingList> index, Map<String, AbstractMap.SimpleEntry<Integer, Integer>> positions){ //positions are indexes, not docids
        int minID = Integer.MAX_VALUE;
        for (PostingList p : index){
            //check if we're in a valid position for the posting list since we may have already processed the entire posting list
            if(positions.get(p.getTerm()).getKey() < p.getPostings().size()){
                minID = Integer.min(minID, p.getPostings().get(positions.get(p.getTerm()).getKey()).getDocId());
            }
        }
        return minID;
    }

    /**
     * Function used to move a pointer inside of positions to the next posting,
     * keeping in mind that we may have to move to the next block
     * @param p the posting list to be moved
     * @param position the current position of the posting list
     * @param block the current block of the posting list
     * @param numBlocks the number of blocks in which the entire posting list is split
     * @param descriptorOffset the offset of the first descriptor block for the posting list
     * @param blockChannel the channel to the block file
     * @return the new position and block of the posting list
     * @throws IOException if there is an error in reading the block file
     */
    public static AbstractMap.SimpleEntry<Integer, Integer> moveToNext(PostingList p, int position,
                                                                       int block, int numBlocks, long descriptorOffset, FileChannel blockChannel) throws IOException {

        //we're not at the end of the current posting list
        if(position + 1 < p.getPostingsLength()){
            return new AbstractMap.SimpleEntry<>(position + 1, block);
        }
        //we're at the end of the current block and not at the last block, so we have to move to the next one
        else if(block + 1 < numBlocks){
            //overwrite the posting list with the next block
            p.rewritePostings(readSkippingBlocks(descriptorOffset + ((long) (block + 1) * SkippingBlock.getEntrySize()), blockChannel).retrieveBlock());
            return new AbstractMap.SimpleEntry<>(0, block + 1);
        }
        else{
            //we're at the end of the posting list
            return null;
        }

    }

    /**
     * Function to compute the tfidf score of a term in a document
     * @param termQueryFrequency how many times the term occurs in the query
     * @param termDocFrequency how many times the term occurs in the document
     * @param termIDF inverse document frequency of the term
     * @return the computed score
     */
    private static double tfIDF(int termQueryFrequency, int termDocFrequency, double termIDF){
        return (termQueryFrequency * ((1 + log10(termDocFrequency)) * (termIDF)));
    }

    /**
     * Function to compute the BM25 score of a term in a document
     * @param termQueryFrequency how many times the term occurs in the query
     * @param termDocFrequency how many times the term occurs in the document
     * @param termIDF inverse document frequency of the term
     * @param docLen length of the document
     * @return
     */
    private static double BM25 (int termQueryFrequency, int termDocFrequency, double termIDF, int docLen){
        return termQueryFrequency * ((termDocFrequency/((k1*((1- b) + (b*docLen/ DocumentIndex.getInstance().getAVDL())))+termDocFrequency)) * (termIDF));
    }

    /**
     * Function to compute the score of a term in a document by specifying the scoring function to use
     * @param isBM25 boolean to specify if we want to use BM25 or tfidf
     * @param termQueryFrequency how many times the term occurs in the query
     * @param termDocFrequency how many times the term occurs in the document
     * @param termIDF inverse document frequency of the term
     * @param docLen length of the document
     * @return
     */
    public static double scoringFunction(boolean isBM25, int termQueryFrequency, int termDocFrequency, double termIDF, int docLen){
        return(isBM25 ? BM25(termQueryFrequency, termDocFrequency, termIDF, docLen) : tfIDF(termQueryFrequency, termDocFrequency, termIDF));
    }


}
