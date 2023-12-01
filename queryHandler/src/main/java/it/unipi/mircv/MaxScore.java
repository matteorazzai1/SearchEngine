package it.unipi.mircv;

import it.unipi.mircv.baseStructure.*;

import java.io.IOException;
import java.util.*;

import static it.unipi.mircv.Utils.scoringFunction;

public class MaxScore {



    public static LinkedList<Map.Entry<Integer, Double>> maxScoreQuery(String query, int k, String scoringFunction) throws IOException {

        HashMap<String, Integer> processedQuery = Utils.queryToDict(query);

        LinkedList<PostingList> postingLists = new LinkedList<>();

        LinkedList<LexiconEntry> lexiconEntries = new LinkedList<>();

        for(Map.Entry<String, Integer> e:processedQuery.entrySet()){
            lexiconEntries.add(Lexicon.retrieveEntryFromDisk(e.getKey()));
            postingLists.add(new PostingList(e.getKey(),PostingList.retrievePostingList(e.getKey())));  //TODO implenting with skipping blocks
        }


        DocumentIndex docIndex = DocumentIndex.getInstance();
        docIndex.loadCollectionStats();
        docIndex.readFromFile();

        //System.out.println(docIndex.getDocsLen().length);

        //priority queue to store the k max scores in ascending order of score
        PriorityQueue<Map.Entry<Integer,Double>> incMaxScoreQueue = new PriorityQueue<>(Map.Entry.comparingByValue());

        //priority queue to store the postingList in ascending order of term upper bound
        PriorityQueue<Map.Entry<PostingList,Double>> sortedPostingLists = new PriorityQueue<>(postingLists.size(), Map.Entry.comparingByValue());

        //lexiconEntries in the same order of the postingList, so sorted in ascending order of term upper bound
        PriorityQueue<Map.Entry<LexiconEntry,Double>> sortedLexiconEntries = new PriorityQueue<>(lexiconEntries.size(), Map.Entry.comparingByValue());

        //map to store the current position of each postingList
        HashMap<PostingList, Integer> positions = new HashMap<>();


        //insert values in the sortedPostingLists
        int i=0;
        for(PostingList pl:postingLists){
            positions.put(pl,0);

            if(scoringFunction.equals("bm25")){
                sortedPostingLists.add(new java.util.AbstractMap.SimpleEntry<>(pl, lexiconEntries.get(i).getMaxBM25()));
                sortedLexiconEntries.add(new java.util.AbstractMap.SimpleEntry<>(lexiconEntries.get(i), lexiconEntries.get(i).getMaxBM25()));
            }
            else {
                sortedPostingLists.add(new java.util.AbstractMap.SimpleEntry<>(pl, lexiconEntries.get(i).getMaxTfidf()));
                sortedLexiconEntries.add(new java.util.AbstractMap.SimpleEntry<>(lexiconEntries.get(i), lexiconEntries.get(i).getMaxBM25()));
            }
            i++;
        }

        //we reorder the LinkedList of lexiconEntries to have it in the same order as the sortedPostingLists
        lexiconEntries=new LinkedList<>();

        for(Map.Entry<LexiconEntry,Double> e:sortedLexiconEntries){
            lexiconEntries.addLast(e.getKey());
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

        //System.out.println("currentDocId: "+currentDocId);

        while(pivot < sortedPostingLists.size() && currentDocId != -1) {

            double partialScore = 0;

            int nextDocId=docIndex.getCollectionSize();

            //Process Essential Lists -------------
            i=0;
            for (Map.Entry<PostingList, Double> e : sortedPostingLists) {

                PostingList pl=e.getKey();

                if(i<pivot){
                    i++;
                    continue;
                }
                //if i>=pivot we are processing an essential list
                if(pl.getPostings()==null || positions.get(pl)==-1){
                    //we have reached the end of the postingList
                    continue;
                }
                if(pl.getPostings().get(positions.get(pl)).getDocId()== currentDocId){

                    if(scoringFunction.equals("bm25")){
                        partialScore+=scoringFunction(true, processedQuery.get(pl.getTerm()), pl.getPostings().get(positions.get(pl)).getFrequency(),
                                lexiconEntries.get(i).getIdf(), docIndex.getDocsLen()[nextDocId-1]);
                    }
                    else {
                        partialScore+=scoringFunction(false, processedQuery.get(pl.getTerm()), pl.getPostings().get(positions.get(pl)).getFrequency(),
                                lexiconEntries.get(i).getIdf(), docIndex.getDocsLen()[nextDocId-1]);
                    }

                    if(positions.get(pl)<pl.getPostings().size()-1) {
                        positions.put(pl, positions.get(pl) + 1);
                    }
                    else{
                        //we have reached the end of the postingList
                        positions.put(pl,-1); //we set the position to -1 to indicate that we have reached the end of the postingList
                        break;
                    }

                    if(pl.getPostings()==null || positions.get(pl)==-1){
                        //we have reached the end of the postingList
                        continue;
                    }

                }


                if(pl.getPostings().get(positions.get(pl)).getDocId()<nextDocId){
                        nextDocId=pl.getPostings().get(positions.get(pl)).getDocId();
                        //System.out.println("nextDocId: "+nextDocId);
                }

                i++;
            }

            //Process Non-Essential Lists -------------
            i=0;
            for (Map.Entry<PostingList, Double> e : sortedPostingLists) {

                PostingList pl = e.getKey();

                if (i == pivot) {
                    break;
                }

                //if i<pivot we are processing a non-essential list
                if(pl.getPostings()==null || positions.get(pl)==-1){
                    //we have reached the end of the postingList
                    continue;
                }

                if (partialScore + ub[i] <= threshold)
                    break;

                //it goes to the position equal to the Doc  //TODO va fatto con nextGEQ?
                while (pl.getPostings().get(positions.get(pl)).getDocId() < currentDocId) {
                    if(positions.get(pl)<pl.getPostings().size()-1) {
                        positions.put(pl, positions.get(pl) + 1);
                    }
                    else{
                        //we have reached the end of the postingList
                        positions.put(pl,-1); //we set the position to -1 to indicate that we have reached the end of the postingList
                        break;
                    }
                }

                if(pl.getPostings()==null || positions.get(pl)==-1){
                    //we have reached the end of the postingList
                    continue;
                }

                if (pl.getPostings().get(positions.get(pl)).getDocId() == currentDocId) {


                    if (scoringFunction.equals("bm25")) {
                        partialScore += scoringFunction(true, processedQuery.get(pl.getTerm()), pl.getPostings().get(positions.get(pl)).getFrequency(),
                                lexiconEntries.get(i).getIdf(), docIndex.getDocsLen()[nextDocId-1]);
                    } else {
                        partialScore += scoringFunction(true, processedQuery.get(pl.getTerm()), pl.getPostings().get(positions.get(pl)).getFrequency(),
                                lexiconEntries.get(i).getIdf(), docIndex.getDocsLen()[nextDocId-1]);
                    }
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

                    while(pivot<sortedPostingLists.size() && ub[pivot]<=threshold){
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

    private static boolean checkReadingPostingLists(HashMap<PostingList, Integer> positions) {
        for(int pos:positions.values()){
            if(pos!=-1){
                return false;
            }
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
