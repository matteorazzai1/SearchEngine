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
import static it.unipi.mircv.Ranking.nextGEQ;
import static it.unipi.mircv.Utils.scoringFunction;
import static it.unipi.mircv.baseStructure.SkippingBlock.readSkippingBlocks;

public class MaxScore {



    public static LinkedList<Map.Entry<Integer, Double>> maxScoreQuery(String query, int k, boolean isBM25) throws IOException {

        HashMap<String, Integer> processedQuery = Utils.queryToDict(query);

        LinkedList<Map.Entry<PostingList, Double>> postingLists = new LinkedList<>();

        LinkedList<LexiconEntry> lexiconEntries = new LinkedList<>();

        FileChannel blocksChannel=(FileChannel) Files.newByteChannel(Paths.get(BLOCK_PATH), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        DocumentIndex docIndex = DocumentIndex.getInstance();

        //System.out.println(docIndex.getDocsLen().length);

        //priority queue to store the k max scores in ascending order of score
        PriorityQueue<Map.Entry<Integer,Double>> incMaxScoreQueue = new PriorityQueue<>(Map.Entry.comparingByValue());

        //priority queue to store the postingList in ascending order of term upper bound
        PriorityQueue<Map.Entry<PostingList,Double>> sortedPostingLists = new PriorityQueue<>(processedQuery.size(), Map.Entry.comparingByValue());

        //lexiconEntries in the same order of the postingList, so sorted in ascending order of term upper bound
        PriorityQueue<Map.Entry<LexiconEntry,Double>> sortedLexiconEntries = new PriorityQueue<>(processedQuery.size(), Map.Entry.comparingByValue());

        //map to store for each postingList the term as key and a couple which indicates position in the block and numBlock
        Map<String, AbstractMap.SimpleEntry<Integer, Integer>> positions = processedQuery.keySet().stream()
                .collect(Collectors.toMap(key -> key, key -> new AbstractMap.SimpleEntry<>(0, 0))); //couple which indicates position in the block and numBlock


        int i=0;
        for(Map.Entry<String, Integer> e:processedQuery.entrySet()){
            LexiconEntry l=Lexicon.retrieveEntryFromDisk(e.getKey());

            PostingList pl=new PostingList(l.getTerm(), readSkippingBlocks(l.getDescriptorOffset(), blocksChannel).retrieveBlock());

            positions.put(pl.getTerm(),new AbstractMap.SimpleEntry<>(0,0));

            if(isBM25){
                sortedPostingLists.add(new java.util.AbstractMap.SimpleEntry<>(pl, l.getMaxBM25()));
                sortedLexiconEntries.add(new java.util.AbstractMap.SimpleEntry<>(l, l.getMaxBM25()));
            }
            else {
                sortedPostingLists.add(new java.util.AbstractMap.SimpleEntry<>(pl, l.getMaxTfidf()));
                sortedLexiconEntries.add(new java.util.AbstractMap.SimpleEntry<>(l, l.getMaxBM25()));
            }
            i++;
        }

        //we generate the LinkedList of lexiconEntries to have the same order as the sortedPostingLists

        for(Map.Entry<LexiconEntry,Double> e:sortedLexiconEntries){
            lexiconEntries.addLast(e.getKey());
        }

        sortedLexiconEntries.clear(); //we clear the priority queue to free memory, we can do that because we have the lexiconEntries in the same order as the sortedLexiconEntries

        //same for postingLists

        for(Map.Entry<PostingList,Double> e:sortedPostingLists){
            postingLists.addLast(e);
        }


        //create array ub to store the upper bounds of the postingLists
        double[] ub=new double[postingLists.size()];
        i=0;
        ub[0]=sortedPostingLists.peek().getValue();
        for (Map.Entry<PostingList, Double> e : sortedPostingLists) {
            if(i>0){
                ub[i]=ub[i-1]+e.getValue();
            }
            i++;
        }


        double threshold = 0;

        int pivot = 0;

        int currentDocId =retrieveSmallestDocId(sortedPostingLists);

        sortedPostingLists.clear(); //we clear the priority queue to free memory, we can do that because we have the postingLists in the same order as the sortedPostingLists


        //System.out.println("currentDocId: "+currentDocId);

        while(pivot < postingLists.size() && currentDocId != -1) {

            double partialScore = 0;

            int nextDocId=docIndex.getCollectionSize();

            //Process Essential Lists -------------
            i=0;
            for (Map.Entry<PostingList, Double> e : postingLists) {

                PostingList pl=e.getKey();

                if(i<pivot){
                    i++;
                    continue;
                }
                //if i>=pivot we are processing an essential list
                if(pl.getPostings()==null || positions.get(pl.getTerm()).getKey()==-1){
                    //we have reached the end of the postingList
                    continue;
                }
                if(pl.getPostings().get(positions.get(pl.getTerm()).getKey()).getDocId()== currentDocId){

                    partialScore+=scoringFunction(isBM25, processedQuery.get(pl.getTerm()), pl.getPostings().get(positions.get(pl.getTerm()).getKey()).getFrequency(),
                            lexiconEntries.get(i).getIdf(), docIndex.getDocsLen()[nextDocId-1]);

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
                            Map.Entry<PostingList,Double> entry=new java.util.AbstractMap.SimpleEntry<>(postingBlock, e.getValue());
                            postingLists.set(i,entry);
                        }
                        else{
                            //we have reached the end of the postingList
                            positions.put(pl.getTerm(),new AbstractMap.SimpleEntry<>(-1,-1)); //we set the position to -1 to indicate that we have reached the end of the postingList
                            break;
                        }
                    }

                    if(pl.getPostings()==null || positions.get(pl.getTerm()).getKey()==-1){
                        //we have reached the end of the postingList
                        continue;
                    }

                }


                if(pl.getPostings().get(positions.get(pl.getTerm()).getKey()).getDocId()<nextDocId){
                        nextDocId=pl.getPostings().get(positions.get(pl.getTerm()).getKey()).getDocId();
                        //System.out.println("nextDocId: "+nextDocId);
                }

                i++;
            }

            //Process Non-Essential Lists -------------
            i=0;
            for (Map.Entry<PostingList, Double> e : postingLists) {

                PostingList pl = e.getKey();

                if (i == pivot) {
                    break;
                }

                //if i<pivot we are processing a non-essential list
                if(pl.getPostings()==null || positions.get(pl.getTerm()).getKey()==-1){
                    //we have reached the end of the postingList
                    continue;
                }

                if (partialScore + ub[i] <= threshold)
                    break;

                AbstractMap.SimpleEntry<Integer, Integer> positionBlockHolder;
                positionBlockHolder = nextGEQ(pl, nextDocId, positions.get(pl.getTerm()).getKey(),
                        positions.get(pl.getTerm()).getValue(), lexiconEntries.get(i).getDescriptorOffset(), lexiconEntries.get(i).getNumBlocks(), blocksChannel);
                if(positionBlockHolder.getKey() == -1){

                    if(positionBlockHolder.getValue() != -1){

                        positions.put(pl.getTerm(), new AbstractMap.SimpleEntry<>(0, positionBlockHolder.getValue()));

                        PostingList postingBlock=new PostingList(pl.getTerm(), readSkippingBlocks(lexiconEntries.get(i).getDescriptorOffset() + (long) positions.get(pl.getTerm()).getValue()
                                *SkippingBlock.getEntrySize(), blocksChannel).retrieveBlock()) ;
                        Map.Entry<PostingList,Double> entry=new java.util.AbstractMap.SimpleEntry<>(postingBlock, e.getValue());
                        postingLists.set(i,entry);
                        break;
                    }
                    else{
                        positions.put(pl.getTerm(), new AbstractMap.SimpleEntry<>(-1,-1));
                        break;
                    }
                }
                else{
                    positions.put(pl.getTerm(), positionBlockHolder);
                }

                if(pl.getPostings()==null || positions.get(pl.getTerm()).getKey()==-1){
                    //we have reached the end of the postingList
                    continue;
                }

                if (pl.getPostings().get(positions.get(pl.getTerm()).getKey()).getDocId() == currentDocId) {

                    partialScore+=scoringFunction(isBM25, processedQuery.get(pl.getTerm()), pl.getPostings().get(positions.get(pl.getTerm()).getKey()).getFrequency(),
                            lexiconEntries.get(i).getIdf(), docIndex.getDocsLen()[nextDocId-1]);
                }

                i++;
            }

            //-----------LIST PIVOT UPDATE----------
            //if the queue does not have k elements or the partialScore calculated is greater than the worst score in the queue in ascending order of score
            //it means that we have to add it to the priority queue
            if(incMaxScoreQueue.size()<k || incMaxScoreQueue.peek().getValue()<partialScore){

                    //System.out.println("added to queue");

                    if(incMaxScoreQueue.size()==k){
                        incMaxScoreQueue.poll();
                    }
                    incMaxScoreQueue.offer(new java.util.AbstractMap.SimpleEntry<>(currentDocId,partialScore));

                    threshold=incMaxScoreQueue.peek().getValue();

                    while(pivot<postingLists.size() && ub[pivot]<=threshold){
                        pivot=pivot+1;
                        //System.out.println("pivot: "+pivot);
                    }
            }

            if(checkReadingPostingLists(positions)){
                break;
            }
            if(nextDocId==currentDocId){
                break;
            }

            currentDocId=nextDocId;
            //System.out.println("currentDocId: "+currentDocId);

        }
        LinkedList<Map.Entry<Integer, Double>> maxScoreQueue = new LinkedList<>();
        while(!incMaxScoreQueue.isEmpty()){
            maxScoreQueue.addFirst(incMaxScoreQueue.poll());
        }
        return maxScoreQueue;
    }

    private static boolean checkReadingPostingLists(Map<String, AbstractMap.SimpleEntry<Integer, Integer>> positions) {
        for(Map.Entry<String, AbstractMap.SimpleEntry<Integer, Integer>> e:positions.entrySet()){
            if(e.getValue().getKey()!=-1)
                return false;
        }
        return true;
    }

    private static int retrieveSmallestDocId(PriorityQueue<Map.Entry<PostingList,Double>> sortedPostingLists) {
        int minDocId = Integer.MAX_VALUE;

        for (Map.Entry<PostingList, Double> e : sortedPostingLists) {
                int docId=e.getKey().getPostings().get(0).getDocId(); //first docId of the postingList

                if(docId<minDocId)
                    minDocId=docId;
        }

        return minDocId;
    }

}
