package it.unipi.mircv;

import it.unipi.mircv.baseStructure.DocumentIndex;
import it.unipi.mircv.baseStructure.LexiconEntry;
import it.unipi.mircv.baseStructure.Posting;
import it.unipi.mircv.baseStructure.PostingList;

import java.util.*;
import java.util.stream.Collectors;

import static it.unipi.mircv.Utils.*;

public class Ranking {

    //index is fine as a list, since we do not need to access a specific entry, while the lexicon entry is needed as
    //Map because we need the IDF of a specific term
    public static PriorityQueue<Map.Entry<Integer, Double>> DAAT(LinkedList<PostingList> index, LinkedList<LexiconEntry> lexiconEntries, String query, boolean isBM25){
        HashMap<String, Integer> processedQuery = queryToDict(query);
        PriorityQueue<Map.Entry<Integer, Double>> finalScores = new PriorityQueue<>(Map.Entry.comparingByValue());
        Map<String, Integer> positions = index.stream()
                .collect(Collectors.toMap(PostingList::getTerm, term -> 0));
        Map<String, Double> lexiconMap = lexiconEntries.stream()
                .collect(Collectors.toMap(LexiconEntry::getTerm, LexiconEntry::getIdf));

        int minID = minDocID(index, (HashMap<String, Integer>) positions);
        double scoreAccumulator;
        Posting currentDoc;

        while(minID != Integer.MAX_VALUE){
            scoreAccumulator = 0;
            for (PostingList p : index){
                if ( p.getPostings().size() > positions.get(p.getTerm())){
                    currentDoc = p.getPostings().get(positions.get(p.getTerm()));
                    if (currentDoc.getDocId() == minID){
                        System.out.println(DocumentIndex.getInstance().getDocs().size());
                        scoreAccumulator += scoringFunction(isBM25, processedQuery.get(p.getTerm()), currentDoc.getFrequency(),
                                lexiconMap.get(p.getTerm()), DocumentIndex.getInstance().getDocs().get(minID).getLength());
                        positions.put(p.getTerm(), positions.get(p.getTerm()) + 1);
                    }
                }
            }
            finalScores.add(new AbstractMap.SimpleEntry<>(minID, scoreAccumulator));
            minID = minDocID(index, (HashMap<String, Integer>) positions);
        }
        return finalScores;
    }

    public static void main(String[] args){
        LinkedList<PostingList> index = new LinkedList<>();
        LinkedList<LexiconEntry> entries = new LinkedList();
        String query = "bejing duck recipe";

        PostingList p1 = new PostingList("bejing\t1:3 2:1");
        PostingList p2 = new PostingList("duck\t2:1 3:3");
        PostingList p3 = new PostingList("recipe\t1:1 3:2");

        index.add(p1);
        index.add(p2);
        index.add(p3);

        LexiconEntry l1 = new LexiconEntry("bejing");
        LexiconEntry l2 = new LexiconEntry("duck");
        LexiconEntry l3 = new LexiconEntry("recipe");

        /*l1.setIDF(log10(15));
        l2.setIDF(log10(8));
        l3.setIDF(log10(12));*/

        entries.add(l1);
        entries.add(l2);
        entries.add(l3);


        System.out.println(DAAT(index, entries, query, false));
        //TODO eliminare setIDF sopra e modificare a riga 32 il parametro doclen
        //


    }
}
