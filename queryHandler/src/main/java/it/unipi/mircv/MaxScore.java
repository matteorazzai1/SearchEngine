package it.unipi.mircv;

import it.unipi.mircv.baseStructure.*;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

import static it.unipi.mircv.Constants.BLOCK_PATH;
import static it.unipi.mircv.Utils.nextGEQ;
import static it.unipi.mircv.Utils.scoringFunction;
import static it.unipi.mircv.baseStructure.SkippingBlock.readSkippingBlocks;

public class MaxScore {



    /**
     * This method returns the k documents with the highest score for a given query with the maxScore algorithm
     * @param query the query that we have to process
     * @param k the number of couple [document,score] to return
     * @param isBM25 if true it uses BM25 as scoring function, otherwise it uses TfIdf
     * @return the list of k documents with the highest score
     * @throws IOException if the file of the blocks is not found
     */
    public static LinkedList<Map.Entry<Integer, Double>> maxScoreQuery(String query, int k, boolean isBM25) throws IOException {

        HashMap<String, Integer> processedQuery = Utils.queryToDict(query);

        FileChannel blocksChannel=(FileChannel) Files.newByteChannel(Paths.get(BLOCK_PATH), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        DocumentIndex docIndex = DocumentIndex.getInstance();

        ArrayList<LexiconEntry> lexiconEntries = new ArrayList<>();

        processedQuery.keySet().parallelStream().forEach(key -> {try {
            LexiconEntry l = LRUCache.retrieveLexEntry(key); //it retrieves the lexiconEntry from the cache if present or from the disk otherwise
            synchronized (lexiconEntries) {
                lexiconEntries.add(l);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }});


        //priority queue to store the k max scores in ascending order of score
        PriorityQueue<Map.Entry<Integer,Double>> incMaxScoreQueue = new PriorityQueue<>(Map.Entry.comparingByValue());


        //map to store for each postingList the term as key and a couple which indicates position in the block and numBlock
        Map<String, AbstractMap.SimpleEntry<Integer, Integer>> positions = processedQuery.keySet().stream()
                .collect(Collectors.toMap(key -> key, key -> new AbstractMap.SimpleEntry<>(0, 0))); //couple which indicates position in the block and numBlock

        ArrayList<PostingList> index = new ArrayList<>();
        

        if(isBM25){
            lexiconEntries.sort(Comparator.comparing(LexiconEntry::getMaxBM25));
        }
        else{
            lexiconEntries.sort(Comparator.comparing(LexiconEntry::getMaxTfidf));
        }

        lexiconEntries.parallelStream().forEachOrdered(l -> {
            try {
                index.add(LRUCache.retrievePostingList(l, blocksChannel));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });


        //create array ub to store the upper bounds of the postingLists
        double[] ub=new double[index.size()];


        if(isBM25)
            ub[0]= (lexiconEntries.get(0).getMaxBM25()*processedQuery.get(lexiconEntries.get(0).getTerm())); //take the value with the lowest upper bound
        else
            ub[0]= (lexiconEntries.get(0).getMaxTfidf()*processedQuery.get(lexiconEntries.get(0).getTerm())); //take the value with the lowest upper bound

        for (int i=1;i<lexiconEntries.size();i++) {

                if(isBM25)
                    ub[i]=ub[i-1]+(lexiconEntries.get(i).getMaxBM25()*processedQuery.get(lexiconEntries.get(i).getTerm()));
                else
                    ub[i]=ub[i-1]+(lexiconEntries.get(i).getMaxTfidf()*processedQuery.get(lexiconEntries.get(i).getTerm()));

        }


        double threshold = 0;

        int pivot = 0;

        int currentDocId =Utils.minDocID(index,positions);


        while(pivot < index.size() && currentDocId != -1) {

            double partialScore = 0;

            int nextDocId=docIndex.getCollectionSize()+1;

            //Process Essential Lists -------------

            for (int i=pivot;i<index.size();i++) {

                PostingList pl=index.get(i);

                //if i>=pivot we are processing an essential list
                if(pl.getPostings()==null || positions.get(pl.getTerm()).getKey()==-1){
                    //we have reached the end of the postingList
                    continue;
                }
                if(pl.getPostings().get(positions.get(pl.getTerm()).getKey()).getDocId()== currentDocId){

                    partialScore+=scoringFunction(isBM25, processedQuery.get(pl.getTerm()), pl.getPostings().get(positions.get(pl.getTerm()).getKey()).getFrequency(),
                            lexiconEntries.get(i).getIdf(), docIndex.getDocsLen()[currentDocId-1]);

                    if((positions.get(pl.getTerm()).getKey())<pl.getPostings().size()-1) {
                        positions.put(pl.getTerm(), new AbstractMap.SimpleEntry<>(positions.get(pl.getTerm()).getKey()+1, positions.get(pl.getTerm()).getValue()));
                    }
                    else{
                        //we have reached the end of the block
                        //we have to check if there are other blocks
                        if(positions.get(pl.getTerm()).getValue()<lexiconEntries.get(i).getNumBlocks()-1) {
                            //we have to read the next block

                            positions.put(pl.getTerm(), new AbstractMap.SimpleEntry<>( 0, positions.get(pl.getTerm()).getValue() + 1));

                            PostingList postingBlock=new PostingList(pl.getTerm(), readSkippingBlocks(lexiconEntries.get(i).getDescriptorOffset() + (long) positions.get(pl.getTerm()).getValue()
                                    *SkippingBlock.getEntrySize(), blocksChannel).retrieveBlock()) ;
                            index.set(i,postingBlock);
                        }
                        else{
                            //we have reached the end of the postingList
                            positions.put(pl.getTerm(),new AbstractMap.SimpleEntry<>(-1,-1)); //we set the position to -1 to indicate that we have reached the end of the postingList

                        }
                    }

                    if(pl.getPostings()==null || positions.get(pl.getTerm()).getKey()==-1){
                        //we have reached the end of the postingList
                        continue;
                    }

                }


                if(pl.getPostings().get(positions.get(pl.getTerm()).getKey()).getDocId()<nextDocId){
                        nextDocId=pl.getPostings().get(positions.get(pl.getTerm()).getKey()).getDocId();
                }

            }

            //Process Non-Essential Lists -------------

            for (int i=pivot-1;i>=0;i--) {


                PostingList pl = index.get(i);


                //if i<pivot we are processing a non-essential list
                if(pl.getPostings()==null || positions.get(pl.getTerm()).getKey()==-1){
                    //we have reached the end of the postingList
                    continue;
                }

                if (partialScore + ub[i] <= threshold)
                    break;

                AbstractMap.SimpleEntry<Integer, Integer> positionBlockHolder;
                positionBlockHolder = nextGEQ(pl, currentDocId, positions.get(pl.getTerm()).getKey(),
                        positions.get(pl.getTerm()).getValue(), lexiconEntries.get(i).getDescriptorOffset(), lexiconEntries.get(i).getNumBlocks(), blocksChannel);

                if(positionBlockHolder==null){
                    positions.put(pl.getTerm(), new AbstractMap.SimpleEntry<>(-1,-1)); //we set the position to -1 to indicate that we have reached the end of the postingList
                }
                else{
                    //update index with the new postingList, updated during nextGEQ
                    index.set(i,pl);
                    //update the position
                    positions.put(pl.getTerm(), positionBlockHolder);
                }

                if(pl.getPostings()==null || positions.get(pl.getTerm()).getKey()==-1){
                    //we have reached the end of the postingList
                    continue;
                }

                if (pl.getPostings().get(positions.get(pl.getTerm()).getKey()).getDocId() == currentDocId) {

                    partialScore+=scoringFunction(isBM25, processedQuery.get(pl.getTerm()), pl.getPostings().get(positions.get(pl.getTerm()).getKey()).getFrequency(),
                            lexiconEntries.get(i).getIdf(), docIndex.getDocsLen()[currentDocId-1]);
                }

            }

            //-----------LIST PIVOT UPDATE----------
            //if the queue does not have k elements, that means that we have to add it to the priority queue
            if(incMaxScoreQueue.size()<k) {
                    incMaxScoreQueue.offer(new java.util.AbstractMap.SimpleEntry<>(currentDocId,partialScore));
            }
            else {
                //the partialScore calculated is greater than the worst score in the queue in ascending order of score, so we have to add it to the priority queue
                if( incMaxScoreQueue.peek().getValue()<partialScore) {
                    incMaxScoreQueue.poll();

                    incMaxScoreQueue.offer(new java.util.AbstractMap.SimpleEntry<>(currentDocId, partialScore));

                }

                threshold=incMaxScoreQueue.peek().getValue();

                while(pivot<index.size() && ub[pivot]<=threshold){
                    pivot=pivot+1;
                }
            }

            if(checkReadingPostingLists(positions)){
                break;
            }
            if(nextDocId==docIndex.getCollectionSize()+1){
                break;
            }

            currentDocId=nextDocId;

        }
        LinkedList<Map.Entry<Integer, Double>> maxScoreQueue = new LinkedList<>();
        while(!incMaxScoreQueue.isEmpty()){
            maxScoreQueue.addFirst(incMaxScoreQueue.poll());
        }
        return maxScoreQueue;
    }

    /**
     * This method checks if all the postingLists have been read completely
     * @param positions is the map that we have to check,it contains for each postingList the term as key and a couple which indicates position in the block and numBlock
     * @return true if all the postingLists have been read completely, false otherwise
     */
    private static boolean checkReadingPostingLists(Map<String, AbstractMap.SimpleEntry<Integer, Integer>> positions) {
        for(Map.Entry<String, AbstractMap.SimpleEntry<Integer, Integer>> e:positions.entrySet()){
            if(e.getValue().getKey()!=-1)
                //if at least one postingList has position set to -1, it means that it has been read completely
                return false;
        }
        return true;
    }


}
